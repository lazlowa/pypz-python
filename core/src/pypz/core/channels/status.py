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
import json
from enum import Enum
from typing import Any, Callable, Optional

from pypz.core.commons.loggers import ContextLogger, DefaultContextLogger
from pypz.core.commons.utils import current_time_millis

NameSeparator = "@"
"""
Used to construct channel unique name
"""


class ChannelFilter:
    """
    This interface contains the methods that shall implement the checking of different
    statuses. This interface can be used to restrict access to ChannelStatusMonitor
    object.
    """

    def is_channel_not_initialised(self) -> bool:
        pass

    def is_channel_opened(self) -> bool:
        pass

    def is_channel_started(self) -> bool:
        pass

    def is_channel_stopped(self) -> bool:
        pass

    def is_channel_closed(self) -> bool:
        pass

    def is_channel_error(self) -> bool:
        pass

    def is_channel_healthy(self) -> bool:
        pass

    def get_channel_unique_name(self) -> str:
        pass

    def get_channel_context_name(self) -> str:
        pass

    def get_channel_group_name(self) -> str:
        pass


class ChannelStatus(Enum):
    Opened = "Opened"
    Started = "Started"
    Stopped = "Stopped"
    Closed = "Closed"
    HealthCheck = "HealthCheck"
    Error = "Error"
    WaitingForSchema = "WaitingForSchema"
    Acknowledged = "Acknowledged"


class ChannelStatusMessage:
    """
    This class is a Data Transfer Object (DTO) for the status messages sent between the channels.

    :param channel_name: name of the channel
    :param channel_context_name: name of the context of the channel, which is the full name of the plugin
    :param channel_group_name: name of the group of the channel, which is the group name of the plugin
    :param channel_group_index: index of the operator in the group, which is inherited by the plugin
    :param status: status
    :param payload: optional payload
    :param timestamp: timestamp of the message at creation
    """

    def __init__(
        self,
        channel_name: str,
        channel_context_name: str,
        channel_group_name: str = None,
        channel_group_index: int = 0,
        status: ChannelStatus | str = None,
        payload: Any = None,
        timestamp: int = None,
    ):
        self.channel_name: str = channel_name
        self.channel_context_name: str = channel_context_name
        self.channel_group_name: str = channel_group_name
        self.channel_group_index: int = channel_group_index
        self.status: ChannelStatus = (
            status if isinstance(status, ChannelStatus) else ChannelStatus[status]
        )
        self.payload: Any = payload
        self.timestamp: int = (
            timestamp if timestamp is not None else current_time_millis()
        )

    def get_channel_unique_name(self):
        return f"{self.channel_name}@{self.channel_context_name}"

    @staticmethod
    def create_from_json(source):
        if isinstance(source, str):
            return ChannelStatusMessage(**json.loads(source))

        if isinstance(source, dict):
            return ChannelStatusMessage(**source)

        raise TypeError(
            "Invalid data type provided for json conversion: " + str(type(source))
        )

    def __str__(self):
        return json.dumps(
            self,
            default=lambda k: (
                list(k)
                if isinstance(k, set)
                else k.__dict__ if not isinstance(k, Enum) else k.value
            ),
        )


class ChannelStatusMonitor(ChannelFilter):
    """
    This class has the responsibility to monitor and track state of the connected channel RW entities.
    If the counterpart sends a status message, this class has to parse and interpret it. In addition,
    it provides an implementation for the ``ChannelFilter`` interface to enable certain queries.

    :param channel_name: name of the channel
    :param channel_context_name: name of the context of the channel, which is the full name of the plugin
    :param channel_group_name: name of the group of the channel, which is the group name of the plugin
    :param logger: the context logger of the plugin, which is inherited from the operator
    """

    ValidPatienceTimeInMs = 120000
    """
    This is the patience time for not receiving any update from the ChannelReader. Once this time
    is up, the ChannelReader will be invalidated
    """

    def __init__(
        self,
        channel_name: str,
        channel_context_name: str,
        channel_group_name: str = None,
        logger: ContextLogger = None,
    ):
        self.channel_name: str = channel_name

        self.channel_context_name: str = channel_context_name

        self.channel_group_name: str = channel_group_name

        self.status_update_map: dict[ChannelStatus, int] = {}

        self.current_status: Optional[ChannelStatus] = None

        self.last_status_change_timestamp_ms: int = 0

        self.last_update_sent_timestamp_ms: int = 0

        self.monitor_start_timestamp: int = current_time_millis()

        self.status_update_callback: Optional[
            Callable[[ChannelStatusMessage], None]
        ] = None

        self.logger: ContextLogger = (
            ContextLogger(logger, *logger.get_context_stack(), channel_context_name)
            if logger is not None
            else ContextLogger(DefaultContextLogger(self.get_channel_unique_name()))
        )

    def get_channel_unique_name(self) -> str:
        return f"{self.channel_name}{NameSeparator}{self.channel_context_name}"

    def get_channel_context_name(self) -> str:
        return self.channel_context_name

    def get_channel_group_name(self) -> str:
        return self.channel_group_name

    def is_channel_healthy(self) -> bool:
        return (
            ChannelStatusMonitor.ValidPatienceTimeInMs
            > (current_time_millis() - self.monitor_start_timestamp)
            if 0 == self.last_update_sent_timestamp_ms
            else ChannelStatusMonitor.ValidPatienceTimeInMs
            > (current_time_millis() - self.last_update_sent_timestamp_ms)
        )

    def is_channel_not_initialised(self):
        return self.current_status is None

    def is_channel_opened(self):
        return self.current_status == ChannelStatus.Opened

    def is_channel_closed(self):
        return self.current_status == ChannelStatus.Closed

    def is_channel_started(self):
        return self.current_status == ChannelStatus.Started

    def is_channel_stopped(self):
        return self.current_status == ChannelStatus.Stopped

    def is_channel_error(self):
        return self.current_status == ChannelStatus.Error

    def on_status_update(self, statusUpdateCallback: Callable):
        self.status_update_callback = statusUpdateCallback

    def update(self, status_message: ChannelStatusMessage):
        """
        This method implements the logic to handle status message updates.
        Note that to overcome messaging issues e.g., missing status update,
        we don't introduce a state machine. It means that most of the states
        can overwrite the actual state with one exception. If "error" status
        has been sent, it can only be overwritten by "open" or "start". This
        allows us to retain error information until the counterpart has been
        restarted.

        :param status_message: the status message received from the other ChannelRW entity
        """

        if status_message.get_channel_unique_name() != self.get_channel_unique_name():
            raise AttributeError(
                f"Invalid status message, "
                f"mismatching channel name: {status_message.get_channel_unique_name()}"
            )

        # int has been merged with long https://www.python.org/dev/peps/pep-0237/
        status_update_time = int(float(status_message.timestamp))

        if status_update_time > self.last_update_sent_timestamp_ms:
            self.last_update_sent_timestamp_ms = status_update_time
        elif status_update_time < self.last_update_sent_timestamp_ms:
            self.logger.warning(
                f"WARNING - the current status message has lower timestamp "
                f"({status_message.timestamp}) than the last received "
                f"({self.last_update_sent_timestamp_ms}). This may be caused by network latency, "
                f"but if you repeatedly see this warning, then you should investigate the causes. "
                f"({status_message})"
            )

        new_status = self.current_status

        if (
            (ChannelStatus.Opened == status_message.status)
            or (ChannelStatus.Closed == status_message.status)
            or (ChannelStatus.Started == status_message.status)
            or (ChannelStatus.Stopped == status_message.status)
            or (ChannelStatus.Error == status_message.status)
        ):
            if status_update_time >= self.last_status_change_timestamp_ms:
                if ChannelStatus.Error != self.current_status:
                    new_status = status_message.status
                elif (ChannelStatus.Opened == status_message.status) or (
                    ChannelStatus.Started == status_message.status
                ):
                    new_status = status_message.status

        if (new_status is None) or (new_status != self.current_status):
            self.logger.debug(
                f"{self.current_status} -> {new_status} @ {status_update_time}"
            )
            self.current_status = new_status
            self.last_status_change_timestamp_ms = status_update_time

        if status_message.payload is not None:
            self.logger.debug(f"Message payload: {status_message.payload}")

        if self.status_update_callback is not None:
            self.status_update_callback(status_message)

        self.status_update_map[status_message.status] = status_update_time
