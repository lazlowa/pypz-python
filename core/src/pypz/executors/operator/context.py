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
from typing import Type, Callable, TypeVar

from pypz.executors.commons import ExecutionMode, ExitCodes
from pypz.core.specs.operator import Operator
from pypz.core.specs.plugin import Plugin
from pypz.core.specs.utils import resolve_dependency_graph

PluginType = TypeVar('PluginType', bound=Plugin)


class ExecutionContext:
    """
    This class is intended to store context information and helper methods for
    the actual execution. The context itself is based on the current Operator.

    :param operator: the actual operator instance to be executed
    :param exec_mode: :class:`ExecutionMode <pypz.executors.commons.ExecutionMode>`
    """

    def __init__(self, operator: Operator, exec_mode: ExecutionMode):
        self.__operator: Operator = operator
        """
        The operator of the context
        """

        self.__exec_mode: ExecutionMode = exec_mode
        """
        The run mode of the execution. For more information refer to the ExecutorRunMode class
        """

        self.__exit_code: ExitCodes = ExitCodes.NoError
        """
        Exit code of the state machine, which can be modified among the states
        """

        self.__plugin_type_registry: dict[Type[PluginType], set[PluginType]] = dict()
        """
        This member holds all the context entities along their implemented interfaces. It allows
        simple type based iteration/execution.
        """

        for nested_instance in self.__operator.get_protected().get_nested_instances().values():
            for spec_class in nested_instance.get_protected().get_spec_classes():
                if spec_class not in self.__plugin_type_registry:
                    self.__plugin_type_registry[spec_class] = set()
                self.__plugin_type_registry[spec_class].add(nested_instance)

        self.__typed_dependency_graphs: dict[Type[PluginType], list[set[PluginType]]] = dict()
        """
        This member holds the nested instances ordered by their resolved dependency list along
        their types. The key holds the instance type, the value is a list of set, where each list
        element represents a dependency level i.e., the 0th element holds the instances w/o
        dependencies, the 1st element holds the instances that are dependent on instances on
        the 0th level and so on.
        """

        for instance_type, instances in self.__plugin_type_registry.items():
            self.__typed_dependency_graphs[instance_type] = resolve_dependency_graph(instances)

    def get_operator(self) -> Operator:
        return self.__operator

    def get_execution_mode(self) -> ExecutionMode:
        return self.__exec_mode

    def get_exit_code(self):
        return self.__exit_code

    def set_exit_code(self, exit_code: ExitCodes):
        self.__exit_code = exit_code

    def get_plugin_instances_by_type(self, plugin_type: Type[PluginType]) -> set[PluginType]:
        return self.__plugin_type_registry[plugin_type] if plugin_type in self.__plugin_type_registry else set()

    def get_dependency_graph_by_type(self, plugin_type: Type[PluginType]) -> list[set[PluginType]]:
        return self.__typed_dependency_graphs[plugin_type] if plugin_type in self.__plugin_type_registry else []

    def for_each_plugin_instances(self, consumer: Callable[[PluginType], None]) -> None:
        for plugin_instance in self.__operator.get_protected().get_nested_instances().values():
            consumer(plugin_instance)

    def for_each_plugin_objects_with_type(self, plugin_type: Type[PluginType],
                                          consumer: Callable[[PluginType], None]) -> None:
        if plugin_type in self.__plugin_type_registry:
            for plugin_instance in self.__plugin_type_registry[plugin_type]:
                consumer(plugin_instance)
