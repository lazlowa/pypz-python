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
from __future__ import annotations
from types import GenericAlias
from typing import Generic, TypeVar, Type, Any, Callable, Optional

from pypz.core.commons.utils import is_type_allowed

allowed_param_types = (str, int, float, set, list, dict, type(None))


ParameterType = TypeVar("ParameterType")


class ExpectedParameter(Generic[ParameterType]):
    """
    This is a descriptor class, which describes an instance parameter. To do that
    you need to describe the parameter on class level (which shall be one of the
    RegisteredInterface) then you need to refer with the same name as instance variable.
    Usage:

    .. code-block:: python

       class TestImpl(Instance):
           required_param = ExpectedParameter(required=True, parameter_type=str)
           optional_param = ExpectedParameter(required=False, parameter_type=str)

           def __init__(self):
               self.required_param = None
               self.optional_param = "defaultValue"

    This is equivalent to:

    .. code-block:: python

       class TestImpl(Instance):
           required_param = RequiredParameter(str)
           optional_param = OptionalParameter(str)

           def __init__(self):
               self.required_param = None
               self.optional_param = "defaultValue"

    :param required: true, if parameter required, false if not
    :param parameter_type: (str, int, float, set, list, dict, type(None))
    :param alt_name: alternative name for the parameter, if specified it acts as reference to the parameter
    :param on_update: callback to react on value update
    """

    NamePrefix = "__private_instance_parameter__"
    """
    This prefix is used to prefix the actual variables created by this descriptor
    """

    def __init__(self,
                 required: bool,
                 parameter_type: Type[ParameterType],
                 alt_name: Optional[str] = None,
                 description: Optional[str] = None,
                 on_update: Callable[[Any, Any], None] = None):

        if parameter_type is None:
            raise TypeError("Parameter type must be specified")

        if "typing" == parameter_type.__module__:
            raise TypeError(f"Type description not allowed: {parameter_type}")

        self.parameter_type: Type[ParameterType] = parameter_type \
            if not isinstance(parameter_type, GenericAlias) else parameter_type.__origin__  # type: ignore

        self.generic_types: list[type] = None \
            if not isinstance(parameter_type, GenericAlias) else parameter_type.__args__  # type: ignore

        if not issubclass(self.parameter_type, allowed_param_types):
            raise TypeError(f"Invalid parameter type: {self.parameter_type}. Allowed types are: {allowed_param_types}")

        self.required: bool = required

        self.name: Optional[str] = alt_name
        """
        Alternative name for the parameter. It can be used to define the actual
        parameter name with having a different variable name.
        """

        self.description: Optional[str] = description
        """
        Short description of the parameter and its intended usage
        """

        self.on_update: Callable[[Any, Any], None] = on_update
        """
        Callback to execute a specific logic, if the value is changed
        """

    def __set_name__(self, owner, name):
        if name.startswith(f"_{owner.__name__}__"):
            raise TypeError("Parameters cannot be defined for private scope")

        if self.name is None:
            self.name = name

        self.internal_name = ExpectedParameter.NamePrefix + name

    def __get__(self, instance, owner) -> ParameterType:
        if not hasattr(instance, self.internal_name):
            raise AttributeError(f"'{owner.__name__}' has no attribute '{self.name}'")

        return getattr(instance, self.internal_name)

    def __set__(self, instance, value):
        if not is_type_allowed(value, allowed_param_types):
            raise TypeError(f"Invalid parameter value type for '{self.name}': {type(value)}. "
                            f"Allowed types: {allowed_param_types}")

        if (value is not None) and \
                (self.parameter_type is not None) and \
                (not isinstance(value, self.parameter_type)):
            raise TypeError(f"Invalid parameter value type for '{self.name}': {type(value)}. "
                            f"Expected: {self.parameter_type}")

        # TODO - evtl. checking against generic parameters

        if (not hasattr(instance, self.internal_name)) and (self.name not in instance.get_protected().get_parameters()):
            """ This is the normal init path, where an object has just been constructed and
                the parameters' initial value is set """

            setattr(instance, self.internal_name, value)
            instance.get_protected().get_parameters()[self.name] = value
            if self.on_update is not None:
                self.on_update(instance, value)
        elif (not hasattr(instance, self.internal_name)) and (self.name in instance.get_protected().get_parameters()):
            """ This path usually represents the case, where a replica is created. The parameter in the dict is
                already set through the original, but the actual parameter attribute is not yet set in the replica.
                In this case it does not matter, what the value is, since the one in the param dict has precedence. """

            setattr(instance, self.internal_name, instance.get_protected().get_parameters()[self.name])
            if self.on_update is not None:
                self.on_update(instance, value)
        elif (hasattr(instance, self.internal_name)) and (self.name in instance.get_protected().get_parameters()):
            """ This is the normal update path for both the normal and the replicated case. Logic shall be
                executed only, if there is value difference. """

            if getattr(instance, self.internal_name) != value:
                setattr(instance, self.internal_name, value)
                if self.on_update is not None:
                    self.on_update(instance, value)
            if instance.get_protected().get_parameters()[self.name] != value:
                """ Note that if parameter has been set directly on the attribute, then
                    we need to update it in the parameters as well. Notice that
                    we cause recurrence, since this __set__ will be called again. This
                    will not cause any trouble however, since this path is not executed
                    if the new value equals old value, which it does, since we set the
                    value in the just before. Recursion is not nice, but still it is
                    the least complex solution to ensure consistency between parameters
                    and ExpectedParameter attributes. """
                instance.get_protected().get_parameters()[self.name] = value
        else:
            """ This path shall actually never be executed, since there is no case, where the value is set, but
                the parameter dict is not updated. This is ensured by the first path. """
            raise AttributeError("Parameter value is missing from the parameter dict. This should not happen."
                                 "Please contact the devs.")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True

        return isinstance(other, type(self)) and \
            (self.required == other.required) and \
            (self.parameter_type == other.parameter_type) and \
            (self.name == other.name) and \
            (self.description == other.description) and \
            (self.generic_types == other.generic_types)

    def to_dict(self, instance=None) -> dict:
        return {
            self.name: {
                'type': None if self.parameter_type is None else
                self.parameter_type.__origin__ if isinstance(self.parameter_type, GenericAlias)  # type: ignore
                else self.parameter_type.__name__,
                'required': self.required,
                'description': self.description,
                'currentValue': None if instance is None else self.__get__(instance, None)
            }
        }


class RequiredParameter(ExpectedParameter[ParameterType]):
    """
    Convenience class to represent a required parameter
    Usage:

    .. code-block::

       class TestImpl(Instance):
           required_param = RequiredParameter(str)
           optional_param = OptionalParameter(str)
           def __init__(self):
               self.required_param = None
               self.optional_param = "defaultValue"

    :param parameter_type: (str, int, float, set, list, dict, type(None))
    :param alt_name: alternative name for the parameter, if specified it acts as reference to the parameter
    :param on_update: callback to react on value update
    """

    def __init__(self,
                 parameter_type: Type[ParameterType] = None,
                 alt_name: str = None,
                 description: Optional[str] = None,
                 on_update: Callable[[Any, Any], None] = None):
        super().__init__(True, parameter_type, alt_name, description, on_update)


class OptionalParameter(ExpectedParameter[ParameterType]):
    """
    Convenience class to represent an optional parameter
    Usage:

    .. code-block::

       class TestImpl(Instance):
           required_param = RequiredParameter(str)
           optional_param = OptionalParameter(str)
           def __init__(self):
               self.required_param = None
               self.optional_param = "defaultValue"

    :param parameter_type: (str, int, float, set, list, dict, type(None))
    :param alt_name: alternative name for the parameter, if specified it acts as reference to the parameter
    :param on_update: callback to react on value update
    """

    def __init__(self,
                 parameter_type: Type[ParameterType] = None,
                 alt_name: str = None,
                 description: Optional[str] = None,
                 on_update: Callable[[Any, Any], None] = None):
        super().__init__(False, parameter_type, alt_name, description, on_update)


def retrieve_parameters(input_val, parameter_type: Type[ExpectedParameter]) -> dict[str, ExpectedParameter]:
    """
    This method attempts to retrieve all described parameters with
    either public or protected scope. Private scoped parameters are
    ignored.

    :param input_val: object or class to be mapped
    :param parameter_type: parameter type to look for
    :return: dict of parameter names to parameter objects
    """

    object_class = input_val.__class__ if not isinstance(input_val, type) else input_val

    result: dict[str, ExpectedParameter] = dict()

    for implemented_class in object_class.__mro__:
        for param_name, param in implemented_class.__dict__.items():
            if isinstance(param, parameter_type) and \
                    (not param_name.startswith(f"_{implemented_class.__name__}__")):  # privates ignored
                if param.name in result:
                    raise AttributeError(f"Parameter '{param.name}' is already "
                                         f"declared in '{implemented_class}'")

                result[param.name] = param

    return result
