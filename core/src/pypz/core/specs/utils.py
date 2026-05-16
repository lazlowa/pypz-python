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
import typing
from importlib import import_module
from typing import Any, Callable, Iterable

IncludedCascadingParameterPrefix = "#"
"""
This strings denotes a cascading parameter. Included cascading parameter means
that the model defines this parameter will get it provided as well the sub instances.
"""

ExcludedCascadingParameterPrefix = ">"
"""
This strings denotes a cascading parameter. Excluded cascading parameter means
that the model defines this parameter will NOT get it provided ONLY the sub instances.
"""


class Internals:
    """
    This helper class realizes a thin wrapper around an Instance object allowing
    the access of protected and private attributes as they were public. It is
    just a convenience feature to hide these getters from the users in a way
    any linter can live with it.

    Notice that it relies on a class variable called '_internal_access', which
    specifies the accessible attributes. If the Instance is being extended, then
    every child classes can define that class variable, since all those fields
    will be resolved in a reverse order dynamically.

    :param instance: the instance object to access internals from
    """

    def __init__(self, instance):
        self._instance = instance

    def __getattr__(self, name):
        allowed = self._collect_allowed_names()
        if name not in allowed:
            raise AttributeError(f"Access to '{name}' is not allowed")

        for cls in type(self._instance).__mro__:
            mangled = f"_{cls.__name__}__{name}"
            if hasattr(self._instance, mangled):
                return getattr(self._instance, mangled)

        protected = f"_{name}"
        if hasattr(self._instance, protected):
            return getattr(self._instance, protected)

        if hasattr(self._instance, name):
            return getattr(self._instance, name)

        raise AttributeError(
            f"'{type(self._instance).__name__}' has no internal attribute '{name}'"
        )

    def __setattr__(self, name, value):
        # Keep wrapper internals on the wrapper itself
        if name == "_instance":
            object.__setattr__(self, name, value)
            return

        allowed = self._collect_allowed_names()
        if name not in allowed:
            raise AttributeError(f"Access to '{name}' is not allowed")

        for cls in type(self._instance).__mro__:
            mangled = f"_{cls.__name__}__{name}"
            if hasattr(self._instance, mangled):
                setattr(self._instance, mangled, value)
                return

        protected = f"_{name}"
        if hasattr(self._instance, protected):
            setattr(self._instance, protected, value)
            return

        if hasattr(self._instance, name):
            setattr(self._instance, name, value)
            return

        raise AttributeError(
            f"'{type(self._instance).__name__}' has no internal attribute '{name}'"
        )

    def _collect_allowed_names(self) -> set[str]:
        """
        This method collects all the available names specified through '_internal_access'
        field on the classes. It resolves them in a reserve order in the inheritance.
        """
        names: set[str] = set()
        for cls in reversed(type(self._instance).__mro__):
            names.update(getattr(cls, "_internal_access", set()))
        return names

    def __dir__(self):
        return sorted(set(super().__dir__()) | self._collect_allowed_names())


class InstanceParameters(dict):
    """
    This class represents the parameters of the instance organized into
    a dictionary. The necessity of extending the builtin dict is that
    the parameter changes shall be watched because the expected parameters
    that are represented by instance attributes shall be updated as well,
    so we need to be able to define callbacks on parameter set.
    The mentioned default callbacks are defined in the Instance class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__update_callbacks: dict[str, list[Callable[[Any], None]]] = {}

    def __setitem__(self, name, value):
        if (name not in self) or (value != self[name]):
            super().__setitem__(name, value)
            if name in self.__update_callbacks:
                for callback in self.__update_callbacks[name]:
                    callback(value)

    @typing.no_type_check
    def update(self, __m, **kwargs) -> None:
        updates = {}

        if __m is not None:
            updates.update(dict(__m))

        updates.update(kwargs)

        for name, value in updates.items():
            self[name] = value

    def on_parameter_update(self, name, callback: Callable[[Any], None]):
        if name not in self.__update_callbacks:
            self.__update_callbacks[name] = []
        self.__update_callbacks[name].append(callback)


def resolve_dependency_graph(instances: set | Iterable) -> list[set]:
    """
    This method can resolve the dependencies along instances provided in the instance set.
    Dependency can be formulated between instances via depends_on(). Resolution means that
    the instances will be ordered into a list of sets, where each outer list represents
    a dependency level and each inner sets hold the instances on that level. For example,
    if there are instances w/o any dependencies, then those will be placed to level 0, which
    is the list[0]. On level 1 all the instances are placed that has dependencies to level 0
    instances and so on.

    :param instances: set of instances to resolve the dependencies across
    :return: list of sets, where the list represents dependency levels and set the instances on it
    """
    instance_set = instances if isinstance(instances, set) else set(instances)

    if 0 < len(instance_set):
        resolved_instances: set = set()
        dependency_level_list: list[set] = []

        for level in range(len(instance_set)):
            # We cannot have more dependency levels than instances we have, hence
            # the outer loop can always create a dependency level, where the instances
            # on the actual level will be stored
            dependency_level_list.append(set())

            for instance in instance_set:
                # It is possible that instances have dependencies that are not present
                # in the list provided as argument, hence we need to create an intersection,
                # so we attempt to resolve dependencies only present in the provided list.
                available_dependencies = Internals(instance).depends_on.intersection(
                    instance_set
                )

                if (instance not in resolved_instances) and (
                    (0 == len(available_dependencies))
                    or available_dependencies.issubset(resolved_instances)
                ):
                    dependency_level_list[level].add(instance)

            resolved_instances.update(dependency_level_list[level])
            if len(resolved_instances) == len(instance_set):
                # Early termination, since all instance dependencies have been resolved
                return dependency_level_list

        # If resolution could not be concluded in N*N steps, then it is very possible
        # that there is a circular dependency. Notice that the method depends_on()
        # checks for circular dependencies, still there is an edge case, where that
        # list is altered outside of that method.
        raise RecursionError("Circular dependency detected in instance dependencies")

    return []


def load_class_by_name(class_name: str) -> type:
    """
    This method loads a class given its name by traversing its module path up
    to the class itself.
    """

    if 0 > class_name.find("."):
        raise ValueError(
            f"Class loading error. Invalid class specifier: {class_name}; "
            f"Class specifier must be composed of [PACKAGE_NAME.]MODULE_NAME:CLASS_NAME[.CLASS_NAME+]"
        )
    trimmed_name = class_name.replace("<class '", "").replace("'>", "")

    loaded = None
    for segment in trimmed_name.split("."):
        if loaded is None:
            loaded = import_module(segment)
        else:
            if hasattr(loaded, segment):
                loaded = getattr(loaded, segment)
            else:
                try:
                    loaded = import_module(loaded.__name__ + "." + segment)
                except ModuleNotFoundError:
                    raise AttributeError(
                        f"Class not found: {loaded.__name__}.{segment}"
                    )

    if not isinstance(loaded, type):
        raise TypeError(f"Specified class name results in a non-type: {loaded}")

    return loaded


def remove_super_classes(classes: set[type]) -> set[type]:
    """
    This function will remove the classes from the set, which are super class for any
    of the others, hence redundant. It is used to clean up the Instance specs to avoid
    Deadly Diamond of Death situation at dynamic class creation.
    """

    return {
        cls
        for cls in classes
        if not any(issubclass(pot, cls) and cls is not pot for pot in classes)
    }
