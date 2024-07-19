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
import time
import traceback
from abc import ABC
from typing import Any, Optional, Type

from pypz.core.channels.io import ChannelReader, ChannelWriter
from pypz.core.channels.status import ChannelStatus
from pypz.core.commons.utils import current_time_millis
from pypz.core.commons.parameters import RequiredParameter, OptionalParameter
from pypz.core.specs.plugin import InputPortPlugin, ResourceHandlerPlugin, OutputPortPlugin, ExtendedPlugin

ParamKeyChannelLocationConfig = "channelLocation"
ParamKeyChannelConfig = "channelConfig"
ParamKeySequentialModeEnabled = "sequentialModeEnabled"
ParamKeyPortOpenTimeoutMs = "portOpenTimeoutMs"
ParamKeySyncConnectionsOpen = "syncConnectionsOpen"


class ChannelInputPort(InputPortPlugin, ResourceHandlerPlugin, ExtendedPlugin, ABC):
    """
    This class represents an abstract InputPortPlugin, which utilizes the channels
    to realize transfer functionalities. It means that once there is a proper
    channel implementation, a fully functional InputPortPlugin can be made by
    providing that implementation in the abstract new_input_channel() method.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    :param schema: the schema of the port plugin, which will be used to send/retrieve data
    :param group_mode: if set to True, the all the input ports in the group shall receive all messages
    :param channel_reader_type: the type of the channel reader to be used
    """

    # ======================= parameter descriptors =======================

    _channel_location = RequiredParameter(str, alt_name=ParamKeyChannelLocationConfig,
                                          description="Location of the channel resource")
    _channel_config = OptionalParameter(dict, alt_name=ParamKeyChannelConfig,
                                        description="Configuration of the channel as dictionary")
    _sequential_mode_enabled = OptionalParameter(bool, alt_name=ParamKeySequentialModeEnabled,
                                                 description="If set to True, then this port will wait with the "
                                                             "processing start until all the connected output "
                                                             "ports are finished")
    _port_open_timeout_ms = OptionalParameter(int, alt_name=ParamKeyPortOpenTimeoutMs,
                                              description="Specifies, how long the port shall wait for incoming"
                                                          "connections")
    _sync_connections_open = OptionalParameter(bool, alt_name=ParamKeySyncConnectionsOpen,
                                               description="If set to True, the port will wait for every expected"
                                                           "output ports to be connected")

    # ======================= ctor =======================

    def __init__(self,
                 name: str = None,
                 schema: Any = None,
                 group_mode: bool = False,
                 channel_reader_type: Type[ChannelReader] = None,
                 *args, **kwargs):
        super().__init__(name, schema, group_mode, *args, **kwargs)

        self.channel_reader_type: Type[ChannelReader] = channel_reader_type
        """
        Type of the channel reader to be created by this plugin.
        """

        self._channel_reader: Optional[ChannelReader] = None
        """
        The only channel reader maintained by this plugin.
        """

        self._interrupted: bool = False
        """
        Helper flag to register, if the execution has been interrupted.
        """

        self._error_occurred: bool = False
        """
        Helper flag to register, if error occurred during execution. This flag will
        be used to prevent resource deletion on error.
        """

        self._need_to_check_connections_opened: bool = True
        """
        Helper flag to signalize, if the channel shall be opened. This is necessary
        to maintain state in _on_port_open().
        """

        self._expected_output_count: Optional[int] = None
        """
        During _on_port_open the expected number of connected outputs will be calculated.
        It then will be used in cases, where we need to wait for all outputs.
        """

        # Expected parameters
        # ===================

        self._channel_location = None
        self._channel_config = dict()
        self._sequential_mode_enabled = False
        self._port_open_timeout_ms = 0
        self._port_open_timeout_start = 0
        self._sync_connections_open = False

    # ======================= method implementations =======================

    def _pre_execution(self) -> None:
        if self.channel_reader_type is None:
            raise AttributeError(f"[{self.get_full_name()}] Channel reader type is not specified")

        if self._channel_reader is None:
            channel_name = self.get_full_name() \
                if self.get_group_principal() is None else self.get_group_principal().get_full_name()
            self._channel_reader = self.channel_reader_type(channel_name, self)
            self._channel_reader.set_location(self._channel_location)
            self._channel_reader.invoke_configure_channel(self._channel_config)

    def _post_execution(self) -> None:
        pass

    def _on_resource_creation(self) -> bool:
        if self.is_principal() and (not self._channel_reader.is_resource_created()):
            if self._channel_reader.invoke_resource_creation():
                self._channel_reader.get_logger().debug("Resource created.")
            else:
                self._channel_reader.get_logger().debug("Waiting for resource creation ...")
                return False
        return True

    def _on_resource_deletion(self) -> bool:
        if self.is_principal() and (not self._channel_reader.is_resource_deleted()):
            # Do not delete resources on error
            if self._error_occurred:
                return True

            if self._channel_reader.invoke_resource_deletion():
                self._channel_reader.get_logger().debug("Resource deleted.")
            else:
                self._channel_reader.get_logger().debug("Waiting for resource deletion ...")
                return False
        return True

    def commit_current_read_offset(self):
        self._channel_reader.invoke_commit_current_read_offset()

    def _on_port_open(self) -> bool:
        if not self._channel_reader.is_channel_open():
            if self._expected_output_count is None:
                self._expected_output_count = sum(output.get_group_size() for output in self.get_connected_ports())

            if self._channel_reader.invoke_open_channel():
                self._channel_reader.get_logger().debug("Channel opened")

                # Resetting, since we reuse this variable at waiting for all inputs to get connected
                self._port_open_timeout_start = 0
            else:
                if 0 < self._port_open_timeout_ms:
                    if 0 == self._port_open_timeout_start:
                        self._port_open_timeout_start = current_time_millis()
                    elif self._port_open_timeout_ms < \
                            (current_time_millis() - self._port_open_timeout_start):
                        raise TimeoutError(f"Timeout exceeded {self._port_open_timeout_ms} [ms]")

                self._channel_reader.get_logger().debug("Waiting for channel to open...")
                return False

        if ChannelReader.NotInitialized == self._channel_reader.get_read_record_offset():
            self._channel_reader.set_initial_record_offset_auto()
            self._channel_reader.get_logger().debug(f"Initialized offset: "
                                                    f"{self._channel_reader.get_read_record_offset()}")

        if not self._channel_reader.is_channel_started():
            self._channel_reader.start_channel()

        self._channel_reader.invoke_sync_status_update()

        # Waiting for outputs to be connected
        # ===================================

        if self._need_to_check_connections_opened:
            if 0 < self._expected_output_count:
                opened_output_count = len(self._channel_reader.retrieve_connected_channel_unique_names(
                    lambda flt: (self.get_group_name() is None) or
                                (flt.get_channel_group_name() != self.get_group_name())
                ))

                if (0 == opened_output_count) or \
                        (self._sync_connections_open and (self._expected_output_count > opened_output_count)):
                    if 0 < self._port_open_timeout_ms:
                        if 0 == self._port_open_timeout_start:
                            self._port_open_timeout_start = current_time_millis()
                        elif self._port_open_timeout_ms < \
                                (current_time_millis() - self._port_open_timeout_start):
                            raise TimeoutError(f"Timeout exceeded {self._port_open_timeout_ms} [ms]")
                    return False

            self._need_to_check_connections_opened = False

        # Waiting for outputs to be finished
        # ==================================
        # Note that normally it does not make sense to slow down the execution by making it sequential,
        # since it is used a couple of times for demo/comparison purposes.
        # Notice that at this point all other relevant checks has been made
        # so there is at least 1 output connected and open

        if self._sequential_mode_enabled:
            if self._channel_reader.is_any_connected_channel_healthy_and_not_stopped_and_not_closed():
                self.get_logger().debug("Waiting for output channel(s) to finish ...")
                return False

        return True

    def retrieve(self) -> Any:
        return self._channel_reader.invoke_read_records()

    def can_retrieve(self) -> bool:
        if not self._channel_reader.is_channel_open():
            raise PermissionError(f"Method allowed only, if channel is open ({self.get_full_name()})")

        finished_output_count = len(self._channel_reader.retrieve_connected_channel_unique_names(
            lambda flt: ((self.get_group_name() is None) or
                         (flt.get_channel_group_name() != self.get_group_name())) and
                        ((not flt.is_channel_healthy()) or flt.is_channel_stopped() or flt.is_channel_closed())
        ))

        return self._channel_reader.has_records() or (self._expected_output_count > finished_output_count)

    def _on_port_close(self) -> bool:
        if self._channel_reader.is_channel_open():
            if self._channel_reader.can_close() and self._channel_reader.invoke_close_channel():
                self._channel_reader.get_logger().debug("Channel closed.")
            else:
                self._channel_reader.get_logger().debug("Waiting for channel to close...")
                return False

        if self.is_principal():
            self.get_logger().debug("Closing port gracefully ...")
            time.sleep(5)
        return True

    def _on_interrupt(self, system_signal: int = None) -> None:
        self.get_logger().warning(f"Interrupted by system signal: {system_signal}")
        self._interrupted = True

    def _on_error(self) -> None:
        self._error_occurred = True
        if self._channel_reader.is_channel_open():
            self._channel_reader.invoke_sync_send_status_message(ChannelStatus.Error)


class ChannelOutputPort(OutputPortPlugin, ResourceHandlerPlugin, ExtendedPlugin, ABC):
    """
    This class represents an abstract OutputPortPlugin, which utilizes the channels
    to realize transfer functionalities. It means that once there is a proper
    channel implementation, a fully functional OutputPortPlugin can be made by
    providing that implementation in the abstract new_output_channel() method.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    :param schema: the schema of the port plugin, which will be used to send/retrieve data
    :param channel_writer_type: the type of the channel writer to be used
    """

    # ======================= parameter descriptors =======================

    _channel_location = RequiredParameter(str, alt_name=ParamKeyChannelLocationConfig,
                                          description="Location of the channel resource")
    _channel_config = OptionalParameter(dict, alt_name=ParamKeyChannelConfig,
                                        description="Configuration of the channel as dictionary")
    _port_open_timeout_ms = OptionalParameter(int, alt_name=ParamKeyPortOpenTimeoutMs,
                                              description="Specifies, how long the port shall wait for incoming"
                                                          "connections")

    # ======================= ctor =======================

    def __init__(self,
                 name: str = None,
                 schema: Any = None,
                 channel_writer_type: Type[ChannelWriter] = None,
                 *args, **kwargs):
        super().__init__(name, schema, *args, **kwargs)

        self.channel_writer_type: Type[ChannelWriter] = channel_writer_type
        """
        Type of the channel writer to be created by this plugin.
        """

        self._channel_writers: Optional[set[ChannelWriter]] = None
        """
        The channel writers to be maintained by this plugin. Note that a channel
        writer will be created for each connection from this output port plugin.
        """

        self._interrupted: bool = False
        """
        Helper flag to register, if the execution has been interrupted.
        """

        self._error_occurred: bool = False
        """
        Helper flag to register, if error occurred during execution. This flag will
        be used to prevent resource deletion on error.
        """

        self._resource_deletion_errors: set[ChannelWriter] = set()
        """
        Set to store the channels that had errors on resource deletion. It is necessary
        to be able not to reschedule those channels in re-execution of the state.
        """

        self._port_close_errors: set[ChannelWriter] = set()
        """
        Set to store the channels that had errors on port closure. It is necessary
        to be able not to reschedule those channels in re-execution of the state.
        """

        # Expected parameters
        # ===================

        self._channel_location = None
        self._channel_config = dict()
        self._port_open_timeout_ms = 0
        self._port_open_timeout_start = 0

    # ======================= method implementations =======================

    def _pre_execution(self) -> None:
        if self.channel_writer_type is None:
            raise AttributeError(f"[{self.get_full_name()}] Channel writer type is not specified")

        if self._channel_writers is None:
            self._channel_writers = set()
            for connection in self.get_connected_ports():
                new_output_channel = self.channel_writer_type(connection.get_full_name(), self)
                new_output_channel.set_location(self._channel_location)
                new_output_channel.invoke_configure_channel(self._channel_config)
                self._channel_writers.add(new_output_channel)

    def _post_execution(self) -> None:
        pass

    def _on_resource_creation(self) -> bool:
        if not self.is_principal():
            return True

        all_resources_created = True

        for channel in self._channel_writers:
            if not channel.is_resource_created():
                if channel.invoke_resource_creation():
                    channel.get_logger().debug("Resource created.")
                else:
                    channel.get_logger().debug("Waiting for resource creation ...")
                    all_resources_created = False

        if not all_resources_created:
            return False

        if 0 == len(self._channel_writers):
            self.get_logger().warning("No channel reader(s) connected")

        return True

    def _on_resource_deletion(self) -> bool:
        if not self.is_principal():
            return True

        all_resources_deleted = True

        for channel in self._channel_writers:
            if (not channel.is_resource_deleted()) and (channel not in self._resource_deletion_errors):
                try:
                    if not self._error_occurred:
                        if channel.invoke_resource_deletion():
                            channel.get_logger().debug("Resource deleted.")
                        else:
                            channel.get_logger().debug("Waiting for resource deletion ...")
                            all_resources_deleted = False
                except:  # noqa: E722
                    # We catch the fact that error occurred, but not interrupting the deletion
                    # process of the other channels
                    self.get_logger().error(traceback.format_exc())
                    self._resource_deletion_errors.add(channel)

        if not all_resources_deleted:
            return False

        if 0 < len(self._resource_deletion_errors):
            raise InterruptedError(f"Error occurred at resource deletion in the following channels: "
                                   f"{[channel.get_channel_name() for channel in self._resource_deletion_errors]}")

        return True

    def _on_port_open(self) -> bool:
        all_channels_opened = True

        for channel in self._channel_writers:
            if not channel.is_channel_open():
                if channel.invoke_open_channel():
                    channel.get_logger().debug("Channel opened.")
                else:
                    channel.get_logger().debug("Waiting for channel open ...")
                    all_channels_opened = False

        if not all_channels_opened:
            if 0 < self._port_open_timeout_ms:
                if 0 == self._port_open_timeout_start:
                    self._port_open_timeout_start = current_time_millis()
                elif self._port_open_timeout_ms < \
                        (current_time_millis() - self._port_open_timeout_start):
                    raise TimeoutError(f"Timeout exceeded {self._port_open_timeout_ms} [ms]")

            return False

        for channel in self._channel_writers:
            if not channel.is_channel_started():
                channel.start_channel()

        return True

    def _on_port_close(self) -> bool:
        all_channels_closed = True

        for channel in self._channel_writers:
            if channel.is_channel_open() and (channel not in self._port_close_errors):
                try:
                    if channel.can_close() and channel.invoke_close_channel():
                        channel.get_logger().debug("Channel closed.")
                    else:
                        all_channels_closed = False
                        channel.get_logger().debug("Waiting for channel to close...")
                except:  # noqa: E722
                    # We catch the fact that error occurred, but not interrupting the shutdown
                    # process of the other channels
                    self.get_logger().error(traceback.format_exc())
                    self._port_close_errors.add(channel)

        if not all_channels_closed:
            return False

        if 0 < len(self._port_close_errors):
            raise InterruptedError(f"Error occurred at port close in the following channels: "
                                   f"{[channel.get_channel_name() for channel in self._port_close_errors]}")

        if self.is_principal():
            self.get_logger().debug("Closing port gracefully ...")
            time.sleep(5)

        return True

    def send(self, data: Any) -> Any:
        for channel in self._channel_writers:
            channel.invoke_write_records(data)

    def _on_interrupt(self, system_signal: int = None) -> None:
        self.get_logger().warning(f"Interrupted by system signal: {system_signal}")
        self._interrupted = True

    def _on_error(self) -> None:
        self._error_occurred = True

        error_channels: set[ChannelWriter] = set()
        for channel in self._channel_writers:
            try:
                if channel.is_channel_open():
                    channel.invoke_sync_send_status_message(ChannelStatus.Error)
            except:  # noqa: E722
                # We catch the fact that error occurred, but not interrupting the error handling
                # process of the other channels
                self.get_logger().error(traceback.format_exc())
                error_channels.add(channel)

        if 0 < len(error_channels):
            raise InterruptedError(f"Error occurred at error handling in the following channels: "
                                   f"{[channel.get_channel_name() for channel in error_channels]}")
