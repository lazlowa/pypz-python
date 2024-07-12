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
import logging
from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, cast, Optional

import yaml

from pypz.core.commons.loggers import ContextLoggerInterface, DefaultContextLogger, ContextLogger
from pypz.core.specs.dtos import PluginInstanceDTO, PluginSpecDTO
from pypz.core.specs.instance import RegisteredInterface, Instance, InstanceGroup

if TYPE_CHECKING:
    from pypz.core.specs.operator import Operator


class Plugin(Instance[None], InstanceGroup, RegisteredInterface, ABC):
    """
    This interface has the only purpose to separate the plugin interfaces
    from other interfaces like the Operator. It is necessary to avoid the
    case, where an Operator could be nested into other Operators. All
    plugin interfaces shall extend this one.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)

        self.__logger: Optional[ContextLogger] = \
            ContextLogger(self.get_context().get_logger(), self.get_full_name()) \
            if self.get_context() is not None else \
            ContextLogger(DefaultContextLogger(self.get_full_name()))
        """
        Context logger, which is the Operator's logger if Operator context existing, otherwise
        it defaults back to the DefaultContextLogger. Note that Plugin without Operator
        context makes only sense in test cases, hence the default log level is set to DEBUG.
        """

        self.__logger.set_log_level(logging.DEBUG)

    # ==================== public methods =======================

    def get_context(self) -> 'Operator':
        return cast('Operator', super().get_context())

    def get_logger(self) -> ContextLogger:
        return self.__logger

    def get_group_size(self) -> int:
        return 1 if self.get_context() is None else self.get_context().get_group_size()

    def get_group_index(self) -> int:
        return 0 if self.get_context() is None else self.get_context().get_group_index()

    def get_group_name(self) -> Optional[str]:
        return None if self.get_group_principal() is None else self.get_group_principal().get_full_name()

    def get_group_principal(self) -> Optional[Instance]:
        if (self.get_context() is None) or (self.get_context().get_group_principal() is None):
            return None
        return self.get_context().get_group_principal().get_protected().get_nested_instance(self.get_simple_name())

    def is_principal(self) -> bool:
        return True if self.get_context() is None else self.get_context().is_principal()

    def get_dto(self) -> PluginInstanceDTO:
        instance_dto = super().get_dto()

        return PluginInstanceDTO(name=instance_dto.name,
                                 parameters=instance_dto.parameters,
                                 dependsOn=instance_dto.dependsOn,
                                 spec=PluginSpecDTO(**instance_dto.spec.__dict__))

    @staticmethod
    def create_from_string(source, *args, **kwargs) -> 'Plugin':
        return Plugin.create_from_dto(PluginInstanceDTO(**yaml.safe_load(source)), *args, **kwargs)

    @staticmethod
    def create_from_dto(instance_dto: 'PluginInstanceDTO', *args, **kwargs) -> 'Plugin':
        return cast(Plugin, Instance.create_from_dto(instance_dto, *args, **kwargs))


class ResourceHandlerPlugin(Plugin, RegisteredInterface, ABC):
    """
    This plugin interface allows to implement resource management related
    functionalities. The respective methods will be called at specific
    times during the execution. Check :ref:`executor` for more information.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)

    @abstractmethod
    def _on_resource_creation(self) -> bool:
        """
        This method shall implement the logic to create an arbitrary resource
        of any type.

        :return: True succeeded, False if more iteration required (to not block the execution)
        """
        pass

    @abstractmethod
    def _on_resource_deletion(self) -> bool:
        """
        This method shall implement the logic to destroy the created resource.

        :return: True succeeded, False if more iteration required (to not block the execution)
        """
        pass


# ====================================== Port Plugins ======================================


class PortPlugin(Plugin, RegisteredInterface, ABC):
    """
    This plugin interface allows to implement common methods for
    port both input and output port plugins.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    :param schema: the schema of the port plugin, which will be used to send/retrieve data
    """

    def __init__(self, name: Optional[str] = None,
                 schema: Any = None,
                 *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.__connected_ports: set[PortPlugin] = self.get_protected().get_reference().__connected_ports \
            if self.get_protected().get_reference() is not None else set()
        """
        This member holds the information about the connected ports, where
        the key is the replication group names and the value is a list of
        connected ports.
        """

        self.__schema: Any = schema
        """
        The port's schema, which is used to identify the format of the data
        sent through the port.
        """

    # ==================== public methods ====================

    def set_schema(self, schema: Any) -> None:
        self.__schema = schema

    def get_schema(self) -> Any:
        return self.__schema

    def get_connected_ports(self) -> set['PortPlugin']:
        return self.__connected_ports

    def connect(self, other_port: 'PortPlugin') -> None:
        # Sanity checks
        # =============

        if isinstance(other_port, type(self)):
            raise TypeError("Invalid connection attempt: ports of same type cannot be connected")

        # Plugins shall have an operator context
        if (self.get_context() is None) or (other_port.get_context() is None):
            raise AttributeError("Invalid port connection attempt: no operator context available")

        # Plugins in the same operator shall not be connected
        if self.get_context() is other_port.get_context():
            raise AttributeError("Invalid port connection attempt. Ports shall have different operator context.")

        # Operators shall have a pipeline context
        if (self.get_context().get_context() is None) or (other_port.get_context().get_context() is None):
            raise AttributeError("Invalid port connection attempt. No pipeline context available.")

        # Operators shall be in the same pipeline context
        if self.get_context().get_context() is not other_port.get_context().get_context():
            raise AttributeError("Invalid port connection attempt. Operators shall be in the same pipeline context.")

        if (self.__schema is not None) and (other_port.__schema is not None) and (self.__schema != other_port.__schema):
            self.get_logger().warning(f"Mismatching schemas. Expected: {self.__schema}; "
                                      f"Provided: {other_port.__schema}")

        self.__connected_ports.add(other_port)
        other_port.__connected_ports.add(self)

    # ==================== overridable methods ====================

    @abstractmethod
    def _on_port_open(self) -> bool:
        """
        This method shall implement the logic to initialize the i/o port functionalities.

        :return: True succeeded, False if more iteration required (to not block the execution)
        """
        pass

    @abstractmethod
    def _on_port_close(self) -> bool:
        """
        This method shall implement the logic to shut down the i/o port functionalities.

        :return: True succeeded, False if more iteration required (to not block the execution)
        """
        pass


class OutputPortPlugin(PortPlugin, RegisteredInterface, ABC):
    """
    This plugin interface allows to implement data transfer output port for an operator.
    Operators can communicate via ports. Different technologies can be implemented
    allowing operators to talk through them.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    :param schema: the schema of the port plugin, which will be used to send/retrieve data
    """

    def __init__(self,
                 name: Optional[str] = None,
                 schema: Optional[Any] = None,
                 *args, **kwargs):
        super().__init__(name, schema, *args, **kwargs)

    @abstractmethod
    def send(self, data: Any) -> Any:
        """
        This method shall implement the logic to send data provided as argument.
        The implementation shall specify the type of the data and the return value.

        :param data: data to be sent
        :return: tbd by the implementation
        """
        pass


class InputPortPlugin(PortPlugin, RegisteredInterface, ABC):
    """
    This plugin interface allows to implement data transfer input port for an operator.
    Operators can communicate via ports. Different technologies can be implemented
    allowing operators to talk through them.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    :param schema: the schema of the port plugin, which will be used to send/retrieve data
    :param group_mode: if set to True, the all the input ports in the group shall receive all messages
    """

    def __init__(self,
                 name: Optional[str] = None,
                 schema: Optional[Any] = None,
                 group_mode: bool = False,
                 *args, **kwargs):
        super().__init__(name, schema, *args, **kwargs)

        self._group_mode: bool = group_mode
        """
        If True, the InputPortPlugin shall receive all records sent to the group
        """

    def is_in_group_mode(self):
        return self._group_mode

    @abstractmethod
    def can_retrieve(self) -> bool:
        """
        This method shall implement the logic to signalize, whether the InputPort is
        still able to retrieve. Unable can mean for example that the OutputPort
        finished writing. This can be then used to terminate reading.

        :return: True if port can retrieve, False if not
        """
        pass

    @abstractmethod
    def retrieve(self) -> Any:
        """
        This method shall implement the logic to retrieve data through the port.

        :return: tbd by the implementation
        """
        pass

    @abstractmethod
    def commit_current_read_offset(self) -> None:
        """
        This method shall implement the logic of committing the current
        read offset based on the technology used.
        """
        pass

# ====================================== Service Plugins ======================================


class ServicePlugin(Plugin, RegisteredInterface, ABC):
    """
    This plugin interface allows to implement a service. Services are special entities
    in the execution, since those are decoupled from the execution life-cycle, hence
    can run in the background. Examples:
    - mounting service, which mounts and unmounts a remote location
    - listener service, which starts a background thread to listen for something
    - etc.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)

    @abstractmethod
    def _on_service_start(self) -> bool:
        """
        This method shall implement the starting logic of the service. You can consider
        services as example like mounting service, which only mounts a folder to the
        system or a background service, which starts a background thread.

        :return: True if logic finished, False if it needs more iteration
        """
        pass

    @abstractmethod
    def _on_service_shutdown(self) -> bool:
        """
        This method shall implement the logic of stopping the service and clean up the
        residuals. E.g., a mounting service could unmount or a background thread could
        be stopped.
        VERY IMPORTANT NOTE - you must always check, if your service start method has
        been called, because it can be that it is never called, if there was an exception
        raised from other entity. However, the shutdown will be called anyway.

        :return: True if logic finished, False if it needs more iteration
        """
        pass


# ====================================== Addons ======================================


class ExtendedPlugin(Plugin, RegisteredInterface, ABC):
    """
    This interface extends the normal plugin's lifecycle. The methods
    defined span outside the execution context, hence it can be used,
    if you need to perform some action before and after execution.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)

    @abstractmethod
    def _pre_execution(self) -> None:
        """
        This method will be called before the executor state machine starts. It
        can be used to perform initialization that is required before the execution.
        """
        pass

    @abstractmethod
    def _post_execution(self) -> None:
        """
        This method is called after the executor and its state machine exited, but
        before the program exits. It can be used to perform finalization/shutdown
        logic that is outside the execution context.
        """
        pass


class LoggerPlugin(Plugin, ContextLoggerInterface, RegisteredInterface, ABC):
    """
    This addon interface allows to implement different logging technologies
    to be used during the execution. Notice that the logger methods are
    coming from the ContextLogger class.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)
