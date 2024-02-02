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
import threading
import time
import traceback
from typing import Callable, Any, TYPE_CHECKING
from abc import ABC, abstractmethod
import concurrent.futures
import json

from pypz.core.channels.status import ChannelStatusMonitor, ChannelFilter, ChannelStatusMessage, ChannelStatus, \
    NameSeparator
from pypz.core.commons.loggers import ContextLogger
from pypz.core.commons.utils import ensure_type, current_time_millis, SynchronizedReference

if TYPE_CHECKING:
    from pypz.core.specs.plugin import PortPlugin


class ChannelMetric:
    def __init__(self, elapsedTimeSinceLastIO, recordCountInLastIO):
        self.elapsedTimeSinceLastIO = elapsedTimeSinceLastIO
        self.recordCountInLastIO = recordCountInLastIO


class ChannelBase(ABC):

    # ======================= Static fields =======================

    DefaultStatusThreadIntervalInMs = 2000
    """
    This value defines in how many ms the status messages (health check) will be sent to the
    counterpart channel.
    """

    ControlLoopExceptionTimeoutInMs = 60000
    """
    This value defines the timeout for having unhandled exception in the control loop.
    After expiration, the control loop will be terminated.    
    """

    ParamKeyLogLevel = "logLevel"
    ParamKeyMetricsEnabled = "metricsEnabled"

    # ======================= Ctor =======================

    def __init__(self, channel_name: str,
                 context: 'PortPlugin',
                 executor: concurrent.futures.ThreadPoolExecutor = None,
                 **kwargs):

        if channel_name is None:
            raise ValueError("Channel name must be provided")

        if context is None:
            raise ValueError("Context instance must be provided")

        self.__silent_mode: bool = kwargs["silent_mode"] if "silent_mode" in kwargs else False
        """
        If this flag is set, then this channel will not send status messages. One use-case is,
        if a channelRW is created to sniff the status of channels.
        """

        self._channel_name: str = channel_name
        """
        This member stores the name of the channel, which normally reflects to the resource's name.
        """

        self._context: PortPlugin = context
        """
        The context of this channel, which shall be an object of PortPlugin type
        """
        
        self._unique_name: str = f"{self._channel_name}{NameSeparator}{self._context.get_full_name()}"
        """
        This name identifies the channel in its context. It is necessary, since channel name on
        its own is not unique, can be reused by different owners.
        """

        self._location: str | None = None
        """
        This member stores the location string of the channel. The location depends on the technology. It can refer
        to either a remote or a local URL as well.
        """

        self._channel_opened: bool = False
        """
        This member is a flag to signalize, whether a channel is open and ready to use.
        """

        self._channel_started: bool = False
        """
        Status flag to signalize, whether the channel has been started.
        """

        self._channel_stopped: bool = False
        """
        Status flag to signalize, whether the channel has been stopped. Note that this additional flag is
        necessary, since having the _channel_started on false does not necessarily mean that the
        output was ever started and is now finished.        
        """

        self._resources_created: bool = False
        """
        Status flag to signalize that the channel's resources are created
        """

        self._resources_deleted: bool = False
        """
        Status flag to signalize that the channel's resources are deleted
        """

        self._executor_started: bool = False
        """
        Flag to signalize, whether the executor thread started
        """

        self._stopping_executor: bool = False
        """
        Flag to signalize the termination attempt of the executor thread
        """

        self._executor_stopped: SynchronizedReference[bool] = SynchronizedReference(False)
        """
        Flag to signalize that the executor thread has been terminated. This flag is necessary along wi
        _stopping_executor to synchronize the termination. Otherwise it can happen that the 
        thread did not terminate yet, but the channel has already been closed resulting in exceptions.
        """

        self._configuration: dict = dict()
        """
        This member stores the configuration string of the channel. This is a kind of serialized storage, since
        the configuration can be an arbitrary type with arbitrary schema as long as the interpretation is knonw.
        """

        self._status_map: dict[str, ChannelStatusMonitor] = dict()
        """
        This map stores the health statuses of all connected channels
        """

        self._executor: concurrent.futures.ThreadPoolExecutor = executor
        """
        This executor is used to execute the background thread, which will invoke the sendHealthStatus method
        """

        self._control_loop_exception_timer: int = 0
        """
        Control loop is, where the control information are sent to the counter channel. Should
        there an exception occur, we need to make sure that we give some time to recover, before
        terminating the channel. This timer defines that grace period.
        """

        self._health_check_payload: dict[str, Any] = dict()
        """
        It is possible to send additional information along with health check events. This
        member serves as the storage of this additional payload.
        """

        self._metrics_enabled: bool = False
        """
        Enables metric calculation i.e., to calculate additional i/o related information
        """

        self._channel_state_update_lock = threading.Lock()
        """
        This lock is used to synchronize the logic that updates channel states
        """

        self._on_status_message_received_callbacks: set[Callable[[list[ChannelStatusMessage]], None]] = set()
        """
        Stores the callbacks, which will be executed, if status messages received
        """

        self._logger: ContextLogger = ContextLogger(self._context.get_logger(),
                                                    self._context.get_full_name(),
                                                    self._channel_name)
        """
        Channel logger
        """

        self._log_level: str = "DEBUG"
        """
        Default log level. This is ok to set here, since if Operator's context logger is present,
        its log level cannot be overriden, but if not, the DefaultContextLogger's can be.
        """

        self._logger.set_log_level(logging.DEBUG)

    # ======================= Abstract methods =======================

    @abstractmethod
    def _create_resources(self) -> bool:
        """
        This method shall implement the logic of creating resources of the channel.
        IMPORTANT NOTE - this method shall be invoked before the open_channel() to make sure that the resources
        are existing beforehand.

        :return: True, if done, False if it is still in progress
        """
        pass

    @abstractmethod
    def _delete_resources(self) -> bool:
        """
        This method shall implement the logic of deleting resources of the channel.
        IMPORTANT NOTE - this method shall be invoked after the close_channel()

        :return: True, if done, False if it is still in progress
        """
        pass

    @abstractmethod
    def _open_channel(self) -> bool:
        """
        This method shall implement the logic to open a channel. The meaning of 'open' however is to be defined
        by the actual implementation. One developer can define it like an opened connection, other as created file etc.

        :return: True, if done, False if it is still in progress
        """
        pass

    @abstractmethod
    def _close_channel(self) -> bool:
        """
        This method shall implement the logic to close a channel. Normally closing a channel is the last step so
        clean up of used resource shall happen here as well.

        :return: True, if done, False if it is still in progress
        """
        pass

    @abstractmethod
    def _configure_channel(self, channel_configuration: dict) -> None:
        """
        This method shall implement the logic to interpret the provided configuration.

        :param channel_configuration: config string
        """
        pass

    @abstractmethod
    def _send_status_message(self, message: str) -> None:
        """
        This method shall implement the logic that publishes the channel's state to the counterpart channel. The
        state string is provided by the channel itself. Note that there is defined schema, how and what will be
        provided by the channel as string, however you can append your own custom information, you only need to
        append as string separated by StateMessageSeparatorChar.

        :param message: message that shall be sent
        """
        pass

    @abstractmethod
    def _retrieve_status_messages(self) -> list:
        """
        This method shall implement the logic that retrieves the status messages published by the counterpart
        channel. Notice that in case of ChannelWriter, there can be multiple InputChannels sending messages and
        in case of ChannelReader there can be multiple messages sent by the ChannelWriter, therefore this method
        shall return a list of messages. Note as well that in case you are using callbacks for your technology,
        you can directly use the method onStatusMessageReceived. In this case simply return null from this method
        to ensure that it will be ignored.

        :return: list of retrieved status messages or null if method is not used
        """
        pass

    # ======================= Getter/setter/invoker methods =======================

    def get_logger(self):
        return self._logger

    def get_channel_name(self):
        return self._channel_name

    def get_unique_name(self):
        return self._unique_name

    def get_location(self):
        return self._location

    def get_context(self) -> 'PortPlugin':
        return self._context

    def get_configuration(self) -> dict:
        return self._configuration

    def is_channel_open(self):
        return self._channel_opened

    def is_channel_started(self):
        return self._channel_started

    def is_channel_stopped(self):
        return self._channel_stopped

    def is_resource_created(self):
        return self._resources_created

    def is_resource_deleted(self):
        return self._resources_deleted

    def set_location(self, channel_location: str):
        self._location = channel_location

    def retrieve_all_connected_channel_count(self) -> int:
        """
        Returns the number of tracked input channels. This can be used to synchronize some activities like starting
        the output.

        :return: number of tracked input channels
        """

        return len(self._status_map)

    def retrieve_connected_channel_unique_names(self, filter_function: Callable[[ChannelFilter], bool] = None) -> set:
        """
        This method returns a set of connected channel names given the evaluation
        criteria passed as argument.

        :param filter_function: check_function check the ChannelFilter for more details (nullable)
        :return: Set of channel names that passes the given evaluation
        """

        connected_channel_names = set()

        for key, val in self._status_map.items():
            if filter_function is not None:
                if filter_function(val):
                    connected_channel_names.add(key)
            else:
                connected_channel_names.add(key)

        return connected_channel_names

    def retrieve_healthy_connected_channel_count(self) -> int:
        """
        Returns the number of healthy tracked input channels.
        This can be used to synchronize some activities like starting the output.

        :return: number of healthy tracked input channels
        """

        healthy_connected_channel_count = 0

        for statusMonitor in list(self._status_map.values()):
            if statusMonitor.is_channel_healthy():
                healthy_connected_channel_count += 1

        return healthy_connected_channel_count

    def is_any_connected_channel_healthy_and_not_stopped(self) -> bool:
        """
        This method realises the question "Is there any connected channel that is healthy, but not
        stopped?". This can be seen as opposite of "Is all connected channels stopped or unhealthy?"
        with the strong difference that the latter does not give a proper answer if there is no
        connected channels at all.

        :return: True if there is any channel healthy, but not stopped, False if not or no connected channel
        """

        for status_monitor in list(self._status_map.values()):
            if status_monitor.is_channel_healthy() and not status_monitor.is_channel_stopped():
                return True
        return False

    def is_any_connected_channel_healthy_and_not_closed(self) -> bool:
        """
        This method realises the question "Is there any connected channel that is healthy, but not
        closed?". This can be seen as opposite of "Is all connected channels closed or unhealthy?"
        with the strong difference that the latter does not give a proper answer if there is no
        connected channels at all.

        :return: True if there is any channel healthy, but not closed, False if not or no connected channel
        """

        for status_monitor in list(self._status_map.values()):
            if status_monitor.is_channel_healthy() and not status_monitor.is_channel_closed():
                return True
        return False

    def is_any_connected_channel_healthy_and_not_stopped_and_not_closed(self) -> bool:
        """
        This method realises the question "Is there any connected channel that is healthy, but not
        stopped and not closed?". This can be seen as opposite of "Is all connected channels closed or
        stopped or unhealthy?" with the strong difference that the latter does not give a proper
        answer if there is no connected channels at all.

        :return: True if there is any channel healthy, but not stopped and not closed,
                 False if not or no connected channel
        """

        for status_monitor in list(self._status_map.values()):
            if status_monitor.is_channel_healthy() and \
                    not status_monitor.is_channel_stopped() and \
                    not status_monitor.is_channel_closed():
                return True
        return False

    def is_any_connected_channels_unhealthy(self) -> bool:
        """
        This method realises the question "Is there any connected channels unhealthy?". This can be seen
        as opposite of "Is all connected channels healthy?" with the strong difference that the latter
        does not give a proper answer if there is no connected channels at all.

        :return: True if there is any channel unhealthy, False if not or no connected channels
        """

        for status_monitor in list(self._status_map.values()):
            if not status_monitor.is_channel_healthy():
                return True
        return False

    def is_any_connected_channels_healthy(self) -> bool:
        """
        This method realises the question "Is there any channel healthy?". This can be seen as opposite
        of "Is all channels unhealthy?" with the strong difference that the latter does not give a proper
        answer if there is no connected channels at all.

        :return: True if there is any channel healthy, False if not or no connected channel
        """
        for status_monitor in list(self._status_map.values()):
            if status_monitor.is_channel_healthy():
                return True
        return False

    def start_channel(self, send_status_message: bool = True) -> None:
        """
        This method sets the corresponding flags and sends the corresponding status message to signalize that
        the channel has been started.

        :param send_status_message: if True the status message sending will be called
        """

        if send_status_message:
            self.invoke_sync_send_status_message(ChannelStatus.Started)

        self._channel_started = True
        self._channel_stopped = False

    def stop_channel(self, send_status_message: bool = True) -> None:
        """
        This method sets the corresponding flags and sends the corresponding status message to signalize that
        the channel has been stopped.

        :param send_status_message: if True the status message sending will be called
        """
        self._channel_started = False
        self._channel_stopped = True

        if send_status_message:
            self.invoke_sync_send_status_message(ChannelStatus.Stopped)

    def can_close(self) -> bool:
        """
        This method can implement the logic to determine, if the channel can
        be closed or not.

        :return: True if the channel can be closed, False otherwise
        """
        return not self.is_any_connected_channel_healthy_and_not_closed()

    def invoke_resource_creation(self) -> bool:
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.

        :return: True if operation done, False if still in progress
        """

        self._resources_created = ensure_type(self._create_resources(), bool)
        return self._resources_created

    def invoke_resource_deletion(self) -> bool:
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.

        :return: True if operation done, False if still in progress
        """

        self._resources_deleted = ensure_type(self._delete_resources(), bool)
        return self._resources_deleted

    def invoke_open_channel(self) -> bool:
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.

        :return: True if operation done, False if still in progress
        """

        if not ensure_type(self._open_channel(), bool):
            return False

        self.invoke_sync_send_status_message(ChannelStatus.Opened)
        self.invoke_sync_status_update()

        if (not self._executor_started) and (not self.__silent_mode):
            if self._executor is None:
                self._executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix=self.get_unique_name())

            self._executor.submit(self.__executor_thread_handler)
            self._executor_started = True

        self._channel_opened = True

        return True

    def invoke_close_channel(self) -> bool:
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.

        :return: True if operation done, False if still in progress
        """

        if not self._channel_opened:
            return True

        if self._executor_started:
            self._stopping_executor = True

            self._logger.debug("Stopping executor thread ...")
            while not self._executor_stopped.get():
                pass
            self._logger.debug("Executor thread stopped")

            if self._executor is not None:
                self._executor.shutdown()
                self._executor = None

        if self._channel_started and not self._channel_stopped:
            try:
                self.stop_channel()
            except Exception:
                self._logger.error("Exception at channel stopping, status message will not be sent.")
                self._logger.error(traceback.format_exc())
                self.stop_channel(False)

        try:
            self.invoke_sync_send_status_message(ChannelStatus.Closed)
        except Exception:
            self._logger.error("Exception at channel closing, status message will not be sent.")
            self._logger.error(traceback.format_exc())

        try:
            self.invoke_sync_status_update()
        except Exception:
            self._logger.error("Exception at status update in closing.")
            self._logger.error(traceback.format_exc())

        # If implemented logic is unfinished then channel is still open
        self._channel_opened = not ensure_type(self._close_channel(), bool)

        # If channel is still open then the closing procedure is unfinished
        return not self._channel_opened

    def invoke_configure_channel(self, channel_configuration: dict) -> None:
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.
        """

        self._configuration.update(channel_configuration)

        self._configure_channel(channel_configuration)

        if ChannelBase.ParamKeyMetricsEnabled in self._configuration:
            self._metrics_enabled = self._configuration[ChannelBase.ParamKeyMetricsEnabled]

        if ChannelBase.ParamKeyLogLevel in self._configuration:
            self._log_level = self._configuration[ChannelBase.ParamKeyLogLevel]

        self._logger.set_log_level(logging.getLevelName(self._log_level))

    def invoke_sync_send_status_message(self, status: ChannelStatus, payload: Any = None) -> None:
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.
        """

        if not self.__silent_mode:
            self._send_status_message(str(ChannelStatusMessage(self._channel_name,
                                                               self._context.get_full_name(),
                                                               self._context.get_group_name(),
                                                               self._context.get_group_index(),
                                                               status, payload)))

    """
    This invoker is just for being consistent with the Java part, since in Java this method is synchronized due
    to the fact that it can be invoked from different threads. In python however this should not be an issue.
    """
    def invoke_sync_status_update(self):
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.
        """

        string_status_messages = self._retrieve_status_messages()

        status_messages: list[ChannelStatusMessage] = list()

        if string_status_messages is not None:
            with self._channel_state_update_lock:
                for string_status_message in string_status_messages:
                    status_message = ChannelStatusMessage.create_from_json(string_status_message)
                    status_messages.append(status_message)

                    # Needs to be made sure that we don't maintain a monitor for ourselves
                    if (status_message.channel_name != self._channel_name) or \
                            (status_message.channel_context_name != self._context.get_full_name()):
                        if status_message.get_channel_unique_name() not in self._status_map:
                            self._status_map[status_message.get_channel_unique_name()] = \
                                ChannelStatusMonitor(status_message.channel_name,
                                                     status_message.channel_context_name,
                                                     status_message.channel_group_name,
                                                     self._logger)

                            self.on_new_channel_status_monitor(
                                self._status_map[status_message.get_channel_unique_name()])

                        self._status_map[status_message.get_channel_unique_name()].update(status_message)

            for callback in self._on_status_message_received_callbacks:
                try:
                    callback(status_messages)
                except Exception as e:
                    self._logger.error(f"Error at status message callback: {e}")
                    self._logger.error(f"{traceback.format_exc()}")

    def on_status_message_received(self, callback: Callable[[list[ChannelStatusMessage]], None]) -> None:
        """
        Adds a callback to the set of callbacks, which will be executed upon status
        message received.

        :param callback: callback Callable[[list[str]], None]
        """

        self._on_status_message_received_callbacks.add(callback)

    def on_new_channel_status_monitor(self, status_monitor: ChannelStatusMonitor) -> None:
        """
        This method can be overridden to handle the creation of the new ChannelStatusMonitor e.g.
        registering callbacks.

        :param status_monitor: ChannelStatusMonitor object newly created
        """
        pass

    def on_status_message_send(self):
        """
        This method can be implemented to hook into the event of sending status message.
        For example one can use it to calculate aggregated metrics between 2 sending and
        attach the metrics to the healthCheckPayload.
        VERY IMPORTANT NOTE: keep the runtime as low as possible, because the higher the
        runtime the more load on the status message sender thread, which might cause
        double activation.
        """
        pass

    # ======================= Meant to be private methods =======================

    """
    This method implements the logic to be executed in the background thread.
    """
    def __executor_thread_handler(self):
        self._logger.debug("Starting status sender thread ...")
        while not self._stopping_executor:

            control_loop_start_time = current_time_millis()

            try:
                self.on_status_message_send()

                self.invoke_sync_send_status_message(ChannelStatus.HealthCheck, self._health_check_payload)

                if self._metrics_enabled:
                    self._logger.debug(f"Metrics: {json.dumps(self._health_check_payload)}")

                self.invoke_sync_status_update()

                # Resetting the patience timer
                self._control_loop_exception_timer = 0
            except Exception:
                if 0 == self._control_loop_exception_timer:
                    self._control_loop_exception_timer = current_time_millis()

                # Note that we ignore all exceptions here, since we cannot risk to close this loop
                # upon a recoverable exception. Hence, we are only plotting the error.
                self._logger.error(f"Unhandled exception in control loop. Timeout in "
                                   f"{current_time_millis() - self._control_loop_exception_timer}/"
                                   f"{ChannelBase.ControlLoopExceptionTimeoutInMs}")
                self._logger.error(traceback.format_exc())

                if ChannelBase.ControlLoopExceptionTimeoutInMs < \
                        (current_time_millis() - self._control_loop_exception_timer):
                    self._stopping_executor = True
            finally:
                control_loop_duration = current_time_millis() - control_loop_start_time

                """
                Calculating the necessary sleep time for the loop. This is necessary in
                order not to block to long, since if the implementation takes 2 sec and
                the loop waits additional 2 secs, then it can miss important information.
                So, we consider the implementation's runtime e.g. if it took 1500ms then,
                the sleep will wait 2000-1500=500ms. Notice that it can never gol below 0.
                """
                if ChannelBase.DefaultStatusThreadIntervalInMs > control_loop_duration:
                    time.sleep((ChannelBase.DefaultStatusThreadIntervalInMs - control_loop_duration) / 1000.0)

        try:
            # Sending one last status message to make sure that all relevant payloads are transferred.
            self.invoke_sync_send_status_message(ChannelStatus.HealthCheck, self._health_check_payload)
        except Exception as ignored:
            self._logger.debug(str(ignored))

        self._executor_stopped.set(True)
        self._stopping_executor = False
        self._executor_started = False
        self._logger.debug("Status sender thread stopped.")
