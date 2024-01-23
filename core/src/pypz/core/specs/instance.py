# =============================================================================
# Copyright (c) 2024 by Laszlo Anka. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================
import inspect
import re
import sys
import types
import typing
from abc import ABCMeta, ABC, abstractmethod
from typing import Type, Any, TypeVar, Generic, Optional

import yaml

from pypz.core.commons.utils import TemplateResolver, is_type_allowed, convert_to_dict
from pypz.core.commons.parameters import retrieve_parameters, ExpectedParameter, allowed_param_types
from pypz.core.specs.dtos import InstanceDTO, SpecDTO
from pypz.core.specs.utils import InstanceParameters, AccessWrapper, load_class_by_name, remove_super_classes, \
    IncludedCascadingParameterPrefix, ExcludedCascadingParameterPrefix


class RegisteredInterface:
    """
    This is a marker interface with the sole purpose to identify the native specs interfaces.
    It is necessary to separate the implemented and multiple inherited classes from the
    actual plugin interfaces.
    """
    pass


class InterceptedInstance:
    """
    This class represents an intercepted instance at init. Part of the feature set is that
    if you don't provide a name to an instance in its parent context, then its variable name
    is used as name. However, since there is no convenient way to provide the name of the variable
    holding the reference to the object into the object's __init__, we need to work around this.
    The concept is that we intercept the instance creation, if no name has been provided through
    a metaclass. This metaclass creates an InterceptedInstance object instead of the real
    object. As next step, the Instance's __setattr__ is modified (since that is the only place,
    where the variable name is given) so that, if it sees that an InterceptedInstance is to
    be assigned, it instead creates the original object, but this time with the name and the
    context object already provided. Hence, all the (nested) instances' __init__ will be
    performed with already available name and context.
    """

    def __init__(self, instance_name: str, context_class: Type['Instance'], *args, **kwargs):
        self.instance_name = instance_name
        self.context_class = context_class
        self.args: tuple = args
        self.kwargs: dict = kwargs


InterceptedInstanceType = TypeVar('InterceptedInstanceType')
"""
This type definition is necessary to allow proper typehints
of the objects created by the Instance class with the custom
metaclass. Without this type definition no hints will be given.
"""


class InstanceInitInterceptor(ABCMeta):
    """
    This metaclass has the responsibility to intercept the instance initialization and so
    ensure that instance name and context objects are resolved automatically, before
    executing the __init__ of the instance. For more information refer to InterceptedInstance.
    """

    @typing.no_type_check
    def __call__(cls: Type[InterceptedInstanceType], name: str = None, *args, **kwargs) -> InterceptedInstanceType:
        """
        This method is called, if a class object is being created, hence we can intercept
        the __init__ call. The following rules are considered for interception:
        - caller frame shall be an object and shall be an instance of Instance
        - name or context object is not provided
        In these cases is the Instance init intercepted, in any other cases a normal
        init invoked.

        :param name: instance name
        :param args: args
        :param kwargs: kwargs
        :return: either InterceptedInstance or normal object
        """

        if ("self" in inspect.currentframe().f_back.f_locals) and \
                isinstance(inspect.currentframe().f_back.f_locals["self"], Instance) and \
                ((name is None) or ("context" not in kwargs)):
            return type.__call__(InterceptedInstance, name, cls, *args, **kwargs)

        if (name is None) or not re.match(r"^[a-zA-Z_]+[a-zA-Z0-9_-]*$", name):
            raise AttributeError(f"Invalid instance name: {name}; "
                                 f"Name must match pattern: ^[a-zA-Z0-9_]+[a-zA-Z0-9_-]*$")

        instance = type.__call__(cls, name, *args, **kwargs)
        instance.__on_init_finished__(*args, **kwargs)
        return instance


NestedInstanceType = TypeVar('NestedInstanceType', bound='Instance')


class Instance(Generic[NestedInstanceType], RegisteredInterface, ABC, metaclass=InstanceInitInterceptor):
    """
    This class represents the instance specs that serves as base for
    all the other models. It abstracts all common features and functionalities
    that is required to maintain the actual instances like parameters
    or even multi level nested instances.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    :param nested_instance_type: type of the expected nested instances
    """

    # ========= ctor ==========

    def __init__(self, name: str = None,
                 nested_instance_type: Type[NestedInstanceType] = None,
                 *args, **kwargs):

        # Sanity checks
        # =============

        if name is not None and not isinstance(name, str):
            raise TypeError(f"Invalid instance name: {name} {type(name)}")

        if nested_instance_type is not None and not issubclass(nested_instance_type, Instance):
            raise TypeError(f"Invalid nested instance type: {nested_instance_type}. "
                            f"Must be an extension of {Instance}")

        # Member declarations
        # ===================

        self.__nested_instance_type: Optional[Type[NestedInstanceType]] = nested_instance_type
        """
        Stores the specified type of the nested instances. This is required to
        be able to discover those instances
        """

        self.__reference = kwargs["reference"] if "reference" in kwargs else None
        """
        Reference to the reference instance. If specified, then some instance internal
        configuration related attributes will refer to the attributes of the reference instance.
        """

        self.__context: Instance = kwargs["context"] if "context" in kwargs else None
        """
        Reference to the context aka parent object. Derived automatically
        in the override implementation of __setattr__.
        """

        self.__nested_instances: dict[str, NestedInstanceType] = dict()
        """
        This dictionary holds all the instances that are nested in the context
        of this instance object
        """

        self.__simple_name: str = name
        """
        Name of the instance, which is represented by the object created from the
        implementation class. If not provided and there is a parent context, then
        the parent context will use the name of the variable.
        """

        self.__full_name: str = self.__simple_name \
            if self.__context is None else self.__context.get_full_name() + "." + self.__simple_name
        """
        Full name of the instance which uniquely identifies it up to its topmost context.
        For example, if an instance A has a parent context B, which has a parent context C,
        then the full name of A is 'C.B.A'. This value is calculated and the result is
        stored after the first calculation to avoid recalculation every time.
        """

        self.__spec_name: str = ":".join([self.__class__.__module__, self.__class__.__qualname__])
        """
        The name of the spec constructed of module and qualified class name. Notice that we separate
        the module name and the class name so that we can identify them at loading by name.
        """

        self.__spec_classes: set = set([
            spec_class for spec_class in self.__class__.__mro__
            if issubclass(spec_class, Instance) and (RegisteredInterface in spec_class.__bases__)
        ])
        """
        Set of specs classes that are in the class hierarchy i.e., which specs classes are
        contributing to the implementation of this class
        """

        self.__expected_parameters: dict[str, ExpectedParameter] = retrieve_parameters(self, ExpectedParameter)
        """
        Map of expected parameters defined as descriptor of the class. Key is the name of the parameter,
        value is the parameter descriptor. Used to check, if an expected (described) parameter value
        shall be set upon instance parameter setting.
        """

        self.__parameters: InstanceParameters = self.__reference.__parameters \
            if self.__reference is not None else InstanceParameters()
        """
        The interpreted instance parameters i.e., cascading and templates are interpreted
        """

        self.__depends_on: set = self.__reference.__depends_on if self.__reference is not None else set()
        """
        Set of other instances that is this instance depending on. Note however that
        the type of the dependencies are checked dynamically in runtime, since dependencies
        can only be defined on the same type of instance.
        """

        """ Registering expected parameter update. This means that, if a parameter has been set via
        the set_parameter() method, we need to make sure that the corresponding parameter object
        is updated as well. """
        for param in self.__expected_parameters.values():
            self.__parameters.on_parameter_update(param.name,
                                                  lambda value, p=param, instance=self: p.__set__(instance, value))

        """ It is clear that in python anything can be accessed, however methods and attributes that
            should not be used are hidden (at least from autocompletion) from the user, still with
            the following dynamically created method it is possible to access them so that the IDE
            is not complaining. """
        object.__setattr__(self, "get_protected", lambda this=self: AccessWrapper(this))

    # ========= overridable methods ==========

    @abstractmethod
    def _on_interrupt(self, system_signal: int = None) -> None:
        """
        This method can be implemented to react to interrupt signals like
        SIGINT, SIGTERM etc. The specs implementation can then execute interrupt
        logic e.g., early termination of loops.

        :param system_signal: id of the system signal that causes interrupt
        """
        pass

    @abstractmethod
    def _on_error(self) -> None:
        """
        This method can be implemented to react to error events during
        execution. The error itself may come from arbitrary sources.
        """
        pass

    # ========= public methods ==========

    def get_context(self):
        return self.__context

    def get_simple_name(self) -> str:
        return self.__simple_name

    def get_full_name(self) -> str:
        return self.__full_name

    def get_parameter(self, name: str):
        return self.__parameters[name]

    def has_parameter(self, name: str) -> bool:
        return name in self.__parameters

    def set_parameter(self, name: str, value: Any) -> None:
        """
        Parameter setter method, which interprets the templates and handles cascading
        parameters. One can ignore setting values for existing parameters via the
        provided flag.

        :param name: parameter name
        :param value: parameter value
        """

        resolved_value = TemplateResolver("${", "}").resolve(value)

        if not is_type_allowed(resolved_value, allowed_param_types):
            raise TypeError(f"Invalid parameter value type for '{name}': {type(resolved_value)}. "
                            f"Allowed types: {allowed_param_types}")

        if name.startswith(ExcludedCascadingParameterPrefix):
            """ Cascade the parameter further. Notice that the first leading
                cascaded specifier has been removed in the parameter name
                Notice that we copy the instances into a list, since there can be
                situations, where the list is modified during iteration via
                replication (e.g., replicationFactor cascaded from pipeline) """
            for nested_instance in list(self.__nested_instances.values()):
                nested_instance.set_parameter(name[1:], resolved_value)
        else:
            param_name = name
            if name.startswith(IncludedCascadingParameterPrefix):
                """ Remove all the cascading specifier from the parameter name """
                param_name = name \
                    .replace(IncludedCascadingParameterPrefix, "") \
                    .replace(ExcludedCascadingParameterPrefix, "")

                """ Cascade the parameter further. Notice that the first leading
                    cascaded specifier has been removed in the parameter name """
                for nested_instance in self.__nested_instances.values():
                    nested_instance.set_parameter(name[1:], resolved_value)

            self.__parameters[param_name] = resolved_value

    def set_parameters(self, parameters: dict) -> None:
        """
        Convenience parameter setter method for dicts, where all key value pair will be
        set separately

        :param parameters: parameter dict
        """

        for name, value in parameters.items():
            self.set_parameter(name, value)

    def get_expected_parameters(self) -> dict | str:
        """
        Returns all the expected parameters as dictionary. Each parameter has the following types:

        .. code-block:: python

           result = {
               'name': {
                   'type': 'str | int | float | set | list | dict | type(None)',
                   'required': 'True | False',
                   'description': 'str',
                   'currentValue': 'str | int | float | set | list | dict | None'
               }
           }
        """

        expected_parameters = dict()
        [expected_parameters.update(param.to_dict(self))
         for param in self.__expected_parameters.values()]

        return expected_parameters

    def get_missing_required_parameters(self) -> dict[str, set[str]]:
        """
        This method returns the missing required parameters recursively
        for all nested instances, hence all parameters will be collected
        in the current scope.

        :return: dict, where key is the instance's full name and value is
                 a list of the names of the missing required parameters. If no missing
                 required parameters, then an empty dict will be returned
        """

        missing_required_parameters = dict()

        # Calling for nested instances recursively
        for instance in self.__nested_instances.values():
            missing_required_parameters.update(instance.get_missing_required_parameters())

        for expected_param in self.__expected_parameters.values():
            if expected_param.required:
                if (expected_param.name not in self.__parameters) or (self.__parameters[expected_param.name] is None):
                    if self.__full_name not in missing_required_parameters:
                        missing_required_parameters[self.__full_name] = set()
                    missing_required_parameters[self.__full_name].add(expected_param.name)

        return missing_required_parameters

    def depends_on(self, instance: 'Instance') -> None:
        """
        Specify dependency instances of the actual instance. The following
        prerequisites shall be considered:
        - dependency must be the same specs type
        - dependency can be defined only in the same parent context
        The same parent context means that if there is an operatorA with plugin1
        and plugin2 and an operatorB with plugin3, then only plugin1 and plugin2
        can express dependencies to each other, since plugin3 is in another operator.

        :param instance: dependency instance
        """

        if not isinstance(instance, Instance):
            raise TypeError(f"[{self.__full_name}] Invalid dependency type: {type(instance)}")

        # Instance cannot depend on self
        if self is instance:
            raise AttributeError(f"[{self.__full_name}] Invalid dependency. Instance cannot depend on itself. ")

        # Makes no sense to express dependency between instances in different context
        if (self.__context is None) or (instance.__context is None) or \
                (self.__context is not instance.__context):
            raise AttributeError(f"[{self.__full_name}] Invalid dependency. "
                                 f"Dependencies must have identical parent context.")

        # Check circular dependency
        if self in instance.__depends_on:
            raise RecursionError(f"Circular dependency between detected: "
                                 f"{self.__full_name} <-> {instance.__full_name} ")

        self.__depends_on.add(instance)

    def get_dto(self) -> InstanceDTO:
        """
        Converts the instance information into the corresponding Data Transfer Object (DTO)

        :return: DTO from instance
        """

        """ This is necessary to avoid the serialization of the extended dict's state and
            to prevent issues, if the parameters on the DTO are altered """
        raw_parameters = dict()
        raw_parameters.update(self.__parameters)

        return InstanceDTO(name=self.__simple_name,
                           parameters=raw_parameters,
                           dependsOn=[instance.get_simple_name() for instance in self.__depends_on],
                           spec=SpecDTO(name=self.__spec_name,
                                        types=[str(spec_class) for spec_class in
                                               remove_super_classes(self.__spec_classes)],
                                        nestedInstanceType=None if self.__nested_instance_type is None
                                        else str(self.__nested_instance_type),
                                        expectedParameters=self.get_expected_parameters(),
                                        nestedInstances=[instance.get_dto() for instance in
                                                         self.__nested_instances.values()]))

    def update(self, source: InstanceDTO | dict | str) -> None:
        """
        This method allows to update certain attributes of the instance based on the
        provided DTO. All attributes can be updated here as well, for which a setter
        exists, in addition one can inject additional nested instances or update existing.

        :param source: json string, dict or DTO
        """

        if isinstance(source, str):
            instance_dto = InstanceDTO(**yaml.safe_load(source))
        elif isinstance(source, dict):
            instance_dto = InstanceDTO(**source)
        elif isinstance(source, InstanceDTO):
            instance_dto = source
        else:
            raise TypeError(f"Invalid update source type: {type(source)}")

        if (instance_dto.name is not None) and (instance_dto.name != self.__simple_name):
            raise ValueError(f"Mismatching instance name provided ({instance_dto.name}) and actual "
                             f"({self.__simple_name}) instance name. ")

        if instance_dto.parameters is not None:
            """ Note that at this point all the nested instances had to be created, hence we update
                the parameters of this instance before the nested instances' so the concept of
                proximity based precedence of parameter setting is honored. """
            self.set_parameters(instance_dto.parameters)

        if instance_dto.spec is not None:
            if (instance_dto.spec.name is not None) and (instance_dto.spec.name != self.__spec_name):
                raise AttributeError(f"[{self.__full_name}] Mismatching specs name. Loaded: {self.__spec_name}; "
                                     f"Provided: {instance_dto.spec.name}")

            if (instance_dto.spec.nestedInstances is not None) and (0 < len(instance_dto.spec.nestedInstances)):
                if self.__nested_instance_type is None:
                    raise AttributeError(f"[{self.get_full_name()}] No nested instance is expected for {type(self)}")

                for nested_instance_dto in instance_dto.spec.nestedInstances:
                    if nested_instance_dto.name is None:
                        raise ValueError("Missing instance name")

                    if nested_instance_dto.name not in self.__nested_instances:
                        raise AttributeError(f"[{self.get_full_name()}] Instance not found with name: "
                                             f"{nested_instance_dto.name}")

                    self.__nested_instances[nested_instance_dto.name].update(nested_instance_dto)

        if instance_dto.dependsOn is not None:
            for instance_name in instance_dto.dependsOn:
                if instance_name not in self.__context.__nested_instances:
                    raise AttributeError(f"[{self.__full_name}] Instance not found in context "
                                         f"'{self.__context.__full_name}': {instance_name}")

                self.depends_on(self.__context.__nested_instances[instance_name])

    # ========= protected methods ==========

    def __on_init_finished__(self, *args, **kwargs) -> None:
        """
        This method can be overridden to implement logic that shall be executed after
        the Instance's __init__ has been finished.
        """

        instance_dto = kwargs["from_dto"] if "from_dto" in kwargs else None

        if instance_dto is not None:
            # Creating not existing nested instances
            if instance_dto.spec is not None:
                if (instance_dto.spec.name is not None) and (instance_dto.spec.name != self.__spec_name):
                    raise AttributeError(f"[{self.__full_name}] Mismatching specs name: {self.__spec_name}; "
                                         f"Expected: {instance_dto.spec.name}")

                if (instance_dto.spec.nestedInstances is not None) and (0 < len(instance_dto.spec.nestedInstances)):
                    if self.__nested_instance_type is None:
                        raise AttributeError(f"[{self.__full_name}] "
                                             f"No nested instance is expected for {type(self)}")

                    for nested_instance_dto in instance_dto.spec.nestedInstances:
                        if nested_instance_dto.name is None:
                            raise ValueError("Missing instance name")

                        # First we need to make sure that all instances are created.
                        # This has to happen before updating the instances, since if
                        # an already existing instance refers to a not yet existing
                        # then error will arise.
                        if nested_instance_dto.name not in self.__nested_instances:
                            nested_instance_reference = self.__reference.__nested_instances[nested_instance_dto.name] \
                                if self.__reference is not None else None
                            new_nested_instance = self.__nested_instance_type.create_from_dto(
                                nested_instance_dto, context=self, reference=nested_instance_reference,
                                mock_nonexistent=True, disable_auto_update=True)

                            if not isinstance(new_nested_instance, self.__nested_instance_type):
                                raise AttributeError(f"[{new_nested_instance.get_full_name()}] "
                                                     f"Mismatching nested instance type. Expected: "
                                                     f"{self.__nested_instance_type}")
                            self.__setattr__(new_nested_instance.__simple_name, new_nested_instance)

    # ========= internal methods ==========

    def __setattr__(self, name, value):
        """
        Overridden method to allow the discovery, registration and linking of
        nested objects. It checks, whether the value is an instance of the
        expected nested instance type, then it retrieves the instance's
        simple name if not given, and then it links the both the parent and
        the nested context to each other

        :param name: name of the variable
        :param value: value of the variable
        """

        if isinstance(value, InterceptedInstance):
            final_instance_name = value.instance_name if value.instance_name is not None else name
            nested_instance_reference = self.__reference.__nested_instances[final_instance_name] \
                if self.__reference is not None else None
            if (self.__nested_instance_type is not None) and \
                    (issubclass(value.context_class, self.__nested_instance_type)):
                instance = value.context_class(final_instance_name, context=self, reference=nested_instance_reference,
                                               *value.args,  **value.kwargs)
            else:
                instance = value.context_class(final_instance_name, context=None, reference=nested_instance_reference,
                                               *value.args, **value.kwargs)
        else:
            instance = value

        object.__setattr__(self, name, instance)

        if hasattr(self, f"_{Instance.__name__}__nested_instance_type") and \
                (self.__nested_instance_type is not None) and \
                isinstance(instance, self.__nested_instance_type) and \
                (instance is not self.__context):
            if instance.get_simple_name() in self.__nested_instances:
                raise AttributeError(f"Instance with name already declared: {instance.get_simple_name()}")

            self.__nested_instances[instance.get_simple_name()] = instance

    def __eq__(self, other):
        if self is other:
            return True

        return isinstance(other, type(self)) and \
            (self.__simple_name == other.__simple_name) and \
            (self.__parameters == other.__parameters) and \
            (self.__depends_on == other.__depends_on) and \
            (self.__nested_instances == other.__nested_instances) and \
            (self.__expected_parameters == other.__expected_parameters) and \
            (self.__spec_name == other.__spec_name)

    def __str__(self):
        return yaml.safe_dump(convert_to_dict(self.get_dto()), default_flow_style=False)

    def __hash__(self):
        return hash((self.__full_name, self.__spec_name))

    def __getattr__(self, name):
        return self.__getattribute__(name)

    # ================= static methods =====================

    @staticmethod
    def create_from_dto(instance_dto: InstanceDTO, *args, **kwargs) -> 'Instance':
        """
        Creates an instance object from the DTO representation. It is capable
        to retrieve and load specified classes and to update created instances
        according to the DTO.

        :param instance_dto: instance DTO
        :return: instance object specified by the DTO
        """

        if not isinstance(instance_dto, InstanceDTO):
            raise TypeError(f"Invalid instance DTO type: {type(instance_dto)}")

        if instance_dto.name is None:
            raise ValueError("Missing instance name")

        if (instance_dto.spec is None) or (instance_dto.spec.name is None):
            raise AttributeError("Invalid or missing 'specs' definition")

        if 0 > instance_dto.spec.name.find(":"):
            raise ValueError(f"Class loading error. Invalid class specifier: {instance_dto.spec.name}; "
                             f"Class specifier must be composed of [PACKAGE_NAME.]MODULE_NAME:CLASS_NAME[.CLASS_NAME+]")

        if instance_dto.spec.location is not None:
            # TODO - remote retrieval shall be implemented here
            raise NotImplementedError("Location based retrieval not yet implemented")

        mock_nonexistent = ("mock_nonexistent" in kwargs) and (kwargs["mock_nonexistent"])

        try:
            instance: Instance = \
                load_class_by_name(instance_dto.spec.name.replace(":", "."))(instance_dto.name,
                                                                             from_dto=instance_dto,
                                                                             *args, **kwargs)
        except (ModuleNotFoundError, AttributeError) as e:
            if mock_nonexistent:
                if (instance_dto.spec.types is None) or (0 == len(instance_dto.spec.types)):
                    raise AttributeError("Missing type specification: instance.spec.types: List[Str]")

                base_classes = set()
                abstract_methods = set()

                # Retrieve and check base classes specified in spec.types
                for class_name in instance_dto.spec.types:
                    loaded_class = load_class_by_name(class_name)
                    if not issubclass(loaded_class, (Instance, RegisteredInterface)):
                        raise TypeError(f"Invalid spec type: {loaded_class}. Expected subclass of {Instance}")
                    base_classes.add(loaded_class)
                    abstract_methods.update(loaded_class.__abstractmethods__)

                nested_instance_type = None if instance_dto.spec.nestedInstanceType is None \
                    else load_class_by_name(instance_dto.spec.nestedInstanceType)

                # Creating dummy implementation of abstract methods
                class_body: dict[str, Any] = {abstract_method: lambda: None for abstract_method in abstract_methods}

                module_and_name = instance_dto.spec.name.split(":")
                class_body["__module__"] = module_and_name[0]
                class_body["mocked"] = True

                # Create new type with name and bases
                instance_type = types.new_class(module_and_name[1],
                                                tuple(remove_super_classes(base_classes)),
                                                {}, lambda ns: ns.update(class_body))

                if "nested_instance_type" in inspect.signature(instance_type.__init__).parameters:  # type: ignore
                    instance = instance_type(instance_dto.name,
                                             nested_instance_type=nested_instance_type,
                                             from_dto=instance_dto, *args, **kwargs)
                else:
                    instance = instance_type(instance_dto.name, from_dto=instance_dto, *args, **kwargs)

                print(f"[WARNING] Mock instance created of spec '{instance_dto.spec.name}' for "
                      f"[{instance.get_full_name()}]. Reason: {e}", file=sys.stderr)
            else:
                raise

        if ("disable_auto_update" not in kwargs) or (not kwargs["disable_auto_update"]):
            instance.update(instance_dto)

        return instance

    @staticmethod
    def create_from_string(source, *args, **kwargs) -> 'Instance':
        """
        Helper method to provide the functionality to create an instance from
        a json model specified either as string or as dict.

        :param source: model as string
        :return: instance object specified by the DTO
        """
        return Instance.create_from_dto(InstanceDTO(**yaml.safe_load(source)), *args, **kwargs)


class InstanceGroup(ABC):
    """
    This class provides methods to access instance group related information. It can be
    implemented on different instance level, since a group might have different meaning
    on top level and on nested level.
    """

    @abstractmethod
    def get_group_size(self) -> int:
        pass

    @abstractmethod
    def get_group_index(self) -> int:
        pass

    @abstractmethod
    def get_group_name(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_group_principal(self) -> Optional['Instance']:
        pass

    @abstractmethod
    def is_principal(self) -> bool:
        pass
