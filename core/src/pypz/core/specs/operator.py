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
import copy
import inspect
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, cast

import yaml
from pypz.core.commons.loggers import ContextLogger, ContextLoggerInterface
from pypz.core.commons.parameters import OptionalParameter
from pypz.core.commons.utils import convert_to_dict
from pypz.core.specs.dtos import (
    OperatorConnection,
    OperatorConnectionSource,
    OperatorInstanceDTO,
    OperatorSpecDTO,
)
from pypz.core.specs.instance import Instance, InstanceGroup, RegisteredInterface
from pypz.core.specs.plugin import (
    InputPortPlugin,
    LoggerPlugin,
    OutputPortPlugin,
    Plugin,
)
from pypz.core.specs.utils import Internals
from wrapt import ObjectProxy

if TYPE_CHECKING:
    from pypz.core.specs.pipeline import Pipeline


class Operator(Instance[Plugin], InstanceGroup, RegisteredInterface, ABC):
    """
    This class represents the operator instance specs. This class shall be used
    to integrate your processing logic with *pypz*. An operator spec can contain
    plugins as nested instance.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    # ========================= inner class =========================

    class Replica(ObjectProxy, InstanceGroup):
        def __init__(self, original: "Operator", replica_index: int):
            super().__init__(original)
            self._self_replica_index: int = replica_index
            self._self_simple_name: str = (
                f"{original.get_simple_name()}_{replica_index}"
            )
            self._self_full_name: str = (
                self._self_simple_name
                if original.get_context() is None
                else original.get_context().get_full_name()
                + "."
                + self._self_simple_name
            )
            self._self_original_internals = Internals(original)

        def materialize(self) -> "Operator":
            original = self.__wrapped__
            replica = original.__class__(
                name=self._self_simple_name,
                replication_origin=original,
                context=original.get_context(),
                replication_group_index=self._self_replica_index + 1,
            )
            materialized_internals = Internals(replica)

            memo: dict[int, object] = {
                id(original): replica,
            }

            for (
                plugin_name,
                original_plugin,
            ) in self._self_original_internals.nested_instances.items():
                materialized_plugin = materialized_internals.nested_instances.get(
                    plugin_name
                )

                if materialized_plugin is not None:
                    memo[id(original_plugin)] = materialized_plugin
                else:
                    memo[id(original_plugin)] = original_plugin.__class__(
                        name=plugin_name, context=replica
                    )

            self._copy_attributes(original, replica, memo)

            return replica

        def get_simple_name(self) -> str:
            return self._self_simple_name

        def get_full_name(self) -> str:
            return self._self_full_name

        def get_original(self) -> "Operator":
            return self.__wrapped__

        def get_dto(self) -> OperatorInstanceDTO:
            """
            Creates a DTO object for the replica. Notice that only operator's simple name is adapted,
            since the underlying plugins don't change names upon replication. Furthermore, notice
            that each replica effectively represents connections to the original, so there is no
            need to update connection details either.
            A replica DTO always describes the principal connection graph.
            """
            dto = self.__wrapped__.get_dto()
            dto.name = self._self_simple_name
            return dto

        def get_group_size(self) -> int:
            return self.__wrapped__.get_group_size()

        def get_group_index(self) -> int:
            return self._self_replica_index + 1

        def get_group_name(self) -> Optional[str]:
            return self.__wrapped__.get_group_name()

        def get_group_principal(self) -> Optional["Instance"]:
            return self.__wrapped__.get_group_principal()

        def is_principal(self) -> bool:
            return False

        def _copy_attributes(self, source, target, memo):
            attr_exclude_prefixes = tuple(
                f"_{clz.__name__}__"
                for clz in source.__class__.mro()
                if RegisteredInterface in clz.__bases__
            )
            attr_includes = (
                "_Instance__parameters",
                "_Instance__depends_on",
                "_PortPlugin__schema",
                "_PortPlugin__connected_ports",
            )
            source_internals = Internals(source)
            target_internals = Internals(target)
            for attr_name, attr_value in source.__dict__.items():
                if (
                    not attr_name.startswith(attr_exclude_prefixes)
                    or (attr_name in attr_includes)
                ) and (attr_name not in target_internals.nested_instances):
                    setattr(
                        target,
                        attr_name,
                        copy.deepcopy(attr_value, memo),
                    )
            for (
                source_plugin_name,
                source_plugin,
            ) in source_internals.nested_instances.items():
                if source_plugin_name not in target_internals.nested_instances:
                    raise AttributeError(
                        f"Replicated plugin ({source_plugin_name}) not found "
                        f"in original operator ({self.__wrapped__.get_full_name()})"
                    )
                self._copy_attributes(
                    source_plugin,
                    target_internals.nested_instances[source_plugin_name],
                    memo,
                )

        def __str__(self):
            return yaml.safe_dump(
                convert_to_dict(self.get_dto()), default_flow_style=False
            )

        def __eq__(self, other):
            """
            The equality check had to be adapted, since this is the only place, where
            both original and replica objects can be represented, hence a common wrapping
            of any super().__eq__ was not an option, otherwise the following equality
            checks returned with inconsistent results:
            - o == r1 -> True (will call :class:`Instance <pypz.core.specs.instance.Instance>`.__eq__)
            - r1 == o -> False (will call :class:`Instance <pypz.core.specs.instance.ReplicaContext>`.__eq__)
            - r1 == r2 -> False (will call :class:`Instance <pypz.core.specs.instance.ReplicaContext>`.__eq__)
            Re-implementing the actual equality check solves this issue.
            """
            if self is other:
                return True

            if not isinstance(other, Operator):
                return False

            return (
                self.get_full_name() == other.get_full_name()
            ) and self.is_equivalent_to(other)

        def __ne__(self, other):
            result = self.__eq__(other)
            if result is NotImplemented:
                return NotImplemented
            return not result

        def __hash__(self):
            """
            This method simply wraps the originals method in replicate context, since
            the get_full_name method is replica-aware.
            """
            return hash((self.get_full_name(), self._self_original_internals.spec_name))

    class Logger(ContextLoggerInterface):
        """
        This is a wrapper class for the logging functionality. It wraps all the
        implementation of the :class:`LoggerPlugin <pypz.core.specs.plugin.LoggerPlugin>`
        and by invoking either method it will invoke the corresponding method of each
        :class:`LoggerPlugin <pypz.core.specs.plugin.LoggerPlugin>`.

        .. note::
           The logger instance is provided to all plugins, so the plugins can call the
           methods of this logger. However, if a :class:`LoggerPlugin <pypz.core.specs.plugin.LoggerPlugin>`
           would invoke the logger methods, it would cause an infinite recursion. This
           is prevented so that in every logger call, the call trace will be analyzed and
           if any instance of a :class:`LoggerPlugin <pypz.core.specs.plugin.LoggerPlugin>`
           is found, then a ``RecursionError`` will be thrown.

        :param logger_plugins: logger plugin instances collected in the operator instance
        """

        def __init__(self, logger_plugins: set[LoggerPlugin]):
            self.__logger_plugins: set[LoggerPlugin] = logger_plugins

        def _error(
            self,
            event: Optional[str] = None,
            context_stack: list[str] = None,
            *args: Any,
            **kw: Any,
        ) -> Any:
            # TODO - maybe we should not care for recursion so we can avoid the execution cost?
            frame = inspect.currentframe()
            while (frame := frame.f_back) is not None:
                if ("self" in frame.f_locals) and (
                    isinstance(frame.f_locals["self"], LoggerPlugin)
                ):
                    raise RecursionError(
                        "Attempted to call operator logger from LoggerAddon. This causes infinite recursion."
                    )

            for logger_plugin in self.__logger_plugins:
                logger_plugin._error(event, context_stack, *args, **kw)

        def _warning(
            self,
            event: Optional[str] = None,
            context_stack: list[str] = None,
            *args: Any,
            **kw: Any,
        ) -> Any:
            # TODO - maybe we should not care for recursion so we can avoid the execution cost?
            frame = inspect.currentframe()
            while (frame := frame.f_back) is not None:
                if ("self" in frame.f_locals) and (
                    isinstance(frame.f_locals["self"], LoggerPlugin)
                ):
                    raise RecursionError(
                        "Attempted to call operator logger from LoggerAddon. This causes infinite recursion."
                    )

            for logger_plugin in self.__logger_plugins:
                logger_plugin._warning(event, context_stack, *args, **kw)

        def _info(
            self,
            event: Optional[str] = None,
            context_stack: list[str] = None,
            *args: Any,
            **kw: Any,
        ) -> Any:
            # TODO - maybe we should not care for recursion so we can avoid the execution cost?
            frame = inspect.currentframe()
            while (frame := frame.f_back) is not None:
                if ("self" in frame.f_locals) and (
                    isinstance(frame.f_locals["self"], LoggerPlugin)
                ):
                    raise RecursionError(
                        "Attempted to call operator logger from LoggerAddon. This causes infinite recursion."
                    )

            for logger_plugin in self.__logger_plugins:
                logger_plugin._info(event, context_stack, *args, **kw)

        def _debug(
            self,
            event: Optional[str] = None,
            context_stack: list[str] = None,
            *args: Any,
            **kw: Any,
        ) -> Any:
            # TODO - maybe we should not care for recursion so we can avoid the execution cost?
            frame = inspect.currentframe()
            while (frame := frame.f_back) is not None:
                if ("self" in frame.f_locals) and (
                    isinstance(frame.f_locals["self"], LoggerPlugin)
                ):
                    raise RecursionError(
                        "Attempted to call operator logger from LoggerAddon. This causes infinite recursion."
                    )

            for logger_plugin in self.__logger_plugins:
                logger_plugin._debug(event, context_stack, *args, **kw)

    # ========================= parameters =========================

    _operator_image_name = OptionalParameter(
        str,
        alt_name="operatorImageName",
        description="The image containing the operator's resources. "
        "It will be used mainly by the deployers.",
    )
    _replication_factor = OptionalParameter(
        int,
        alt_name="replicationFactor",
        description="Determines, how many replicas "
        "shall be created from the original.",
        on_update=lambda instance, val: instance.__replicate(),
    )

    # ========================= ctor =========================

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Plugin, *args, **kwargs)

        self.__replication_origin: Optional[Operator] = kwargs.get(
            "replication_origin", None
        )
        """
        Reference to the original instance, which was the base for the replication
        """

        self.__replicas: list[Operator.Replica] = []
        """
        List of replica instances
        """

        self._replication_factor: int = 0
        """
        PARAMETER - The replication factor specifies, how many replicas shall be created along
        the original instance.
        """

        self._operator_image_name: Optional[str] = None
        """
        PARAMETER - Name of the docker image, where the operator can be found. This is optional for the execution,
        but is required at deployment.
        """

        self.__replication_group_index: int = (
            0
            if "replication_group_index" not in kwargs
            else kwargs["replication_group_index"]
        )
        """
        Specifies the group index, if the operator is replicated.
        """

        self.__logger_plugins: set[LoggerPlugin] = set()
        """
        Collection of logger addons to store for context logging
        """

        self.__logger: ContextLogger = ContextLogger(
            Operator.Logger(self.__logger_plugins), self.get_full_name()
        )
        """
        Context logger. For more information refer to Operator.Logger class
        """

    # ==================== implementable methods ====================

    @abstractmethod
    def _on_init(self) -> bool:
        """
        This method shall implement the logic to initialize the operation.
        It will be called after services are started and resources are created.

        :return:

        - True, if finished
        - False, if more iteration required (to not block the execution)
        """
        pass

    @abstractmethod
    def _on_running(self) -> Optional[bool]:
        """
        This method shall implement the actual business logic.
        It will be called after the _on_init has successfully finished

        :return:

        - True, if finished
        - False, if more iteration required (to not block the execution)
        - None, if automatically to be determined based on all inputs i.e., if no more input record, then finish
        """
        pass

    @abstractmethod
    def _on_shutdown(self) -> bool:
        """
        This method shall implement the logic to shut down the operation.
        It will be called, after the _on_running has successfully finished.

        :return:

        - True, if finished
        - False, if more iteration required (to not block the execution)
        """
        pass

    # ==================== public methods ====================

    def get_context(self) -> "Pipeline":
        return cast("Pipeline", super().get_context())

    def get_logger(self) -> ContextLogger:
        return self.__logger

    def get_replication_factor(self):
        return self._replication_factor

    def get_operator_image_name(self):
        return self._operator_image_name

    def get_group_size(self) -> int:
        return self._replication_factor + 1

    def get_group_index(self) -> Optional[int]:
        return self.__replication_group_index

    def get_group_name(self) -> Optional[str]:
        """
        If an Operator instance is replicated, then the replication group name
        is the actual instance name of the original instance.

        :return: the replication group name
        """

        return (
            None
            if self.__replication_origin is None
            else self.__replication_origin.get_full_name()
        )

    def get_group_principal(self) -> Optional[Instance]:
        return self.__replication_origin

    def is_principal(self) -> bool:
        return (self.__replication_origin is None) or (
            self.__replication_origin is self
        )

    def get_replica(self, replica_id: int) -> "Operator.Replica":
        """
        Returns the replica instance by id. The id is the actual place in the
        replica list, which is ensured during the replica creation.

        :param replica_id: replica id
        :return: replica specified by the id
        """
        return self.__replicas[replica_id]

    def get_replicas(self) -> list["Operator.Replica"]:
        """
        :return: replica list
        """

        return self.__replicas

    def get_dto(self) -> OperatorInstanceDTO:
        connections = []
        for instance in Internals(self).nested_instances.values():
            if isinstance(instance, InputPortPlugin):
                for connected_port in instance.get_connected_ports():
                    connections.append(
                        OperatorConnection(
                            inputPortName=instance.get_simple_name(),
                            source=OperatorConnectionSource(
                                instanceName=connected_port.get_context().get_simple_name(),
                                outputPortName=connected_port.get_simple_name(),
                            ),
                        )
                    )

        instance_dto = super().get_dto()

        return OperatorInstanceDTO(
            name=instance_dto.name,
            parameters=instance_dto.parameters,
            dependsOn=instance_dto.dependsOn,
            spec=OperatorSpecDTO(**instance_dto.spec.__dict__),
            connections=connections,
        )

    def update(self, source: OperatorInstanceDTO | dict | str) -> None:
        """
        Overridden to allow connection and replica updates.
        """

        if isinstance(source, str):
            instance_dto = OperatorInstanceDTO(**yaml.safe_load(source))
        elif isinstance(source, dict):
            instance_dto = OperatorInstanceDTO(**source)
        elif isinstance(source, OperatorInstanceDTO):
            instance_dto = source
        else:
            raise TypeError(f"Invalid update source type: {type(source)}")

        super().update(instance_dto)

        # Update connections
        # ==================

        if instance_dto.connections and self.get_context():
            internals = Internals(self)
            for connection in instance_dto.connections:
                if connection.inputPortName not in internals.nested_instances:
                    raise AttributeError(
                        f"[{self.get_full_name()}] Invalid update: InputPort plugin not found "
                        f"with name '{connection.inputPortName}'"
                    )

                context_internals = Internals(self.get_context())

                if (
                    connection.source.instanceName
                    not in context_internals.nested_instances
                ):
                    raise AttributeError(
                        f"[{self.get_full_name()}] Invalid update: source instance not found "
                        f"in pipeline with name '{connection.source.instanceName}'"
                    )

                source_instance = context_internals.nested_instances[
                    connection.source.instanceName
                ]
                source_internals = Internals(source_instance)

                if (
                    connection.source.outputPortName
                    not in source_internals.nested_instances
                ):
                    raise AttributeError(
                        f"[{self.get_full_name()}] Invalid update: OutputPort plugin not found "
                        f"in source instance '{connection.source.instanceName}' "
                        f"with name '{connection.source.outputPortName}'"
                    )

                input_port_plugin: InputPortPlugin = cast(
                    InputPortPlugin,
                    internals.nested_instances[connection.inputPortName],
                )
                output_port_plugin: OutputPortPlugin = cast(
                    OutputPortPlugin,
                    source_internals.nested_instances[connection.source.outputPortName],
                )

                input_port_plugin.connect(output_port_plugin)

    def __replicate(self):
        if not self.is_principal():
            return

        if 0 > self._replication_factor:
            raise ValueError(
                f"Replication factor cannot be negative: {self._replication_factor}"
            )

        difference = self._replication_factor - len(self.__replicas)

        if 0 == difference:
            return

        if 0 < difference:
            if self.__replication_origin is None:
                self.__replication_origin = self
                self.__replication_group_index = 0

            for idx in range(len(self.__replicas), self._replication_factor):
                replica = Operator.Replica(self, idx)

                if self.get_context() is not None:
                    self.get_context().__setattr__(replica.get_simple_name(), replica)

                self.__replicas.append(replica)
        else:
            replicas_to_remove = self.__replicas[self._replication_factor :]

            for replica in replicas_to_remove:
                self.__replicas.pop()
                if self.get_context() is not None:
                    del Internals(self.get_context()).nested_instances[
                        replica.get_simple_name()
                    ]
                    self.get_context().__delattr__(replica.get_simple_name())

            if 0 == len(self.__replicas):
                self.__replication_origin = None
                self.__replication_group_index = 0

    # ==================== protected methods ====================

    def __on_init_finished__(self, *args, **kwargs):
        super().__on_init_finished__(*args, **kwargs)

        # Logger addon handling
        # =====================
        for nested_instance in Internals(self).nested_instances.values():
            if isinstance(nested_instance, LoggerPlugin):
                self.__logger_plugins.add(nested_instance)

    # ========= static methods ==========

    @staticmethod
    def create_from_dto(
        instance_dto: OperatorInstanceDTO, *args, **kwargs
    ) -> "Operator":
        return cast(Operator, Instance.create_from_dto(instance_dto, *args, **kwargs))

    @staticmethod
    def create_from_string(source, *args, **kwargs) -> "Operator":
        return Operator.create_from_dto(
            OperatorInstanceDTO(**yaml.safe_load(source)), *args, **kwargs
        )
