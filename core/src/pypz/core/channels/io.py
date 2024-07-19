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
import concurrent.futures
from typing import Any, TYPE_CHECKING, Optional
from abc import abstractmethod
import threading

from pypz.core.channels.base import ChannelBase, ChannelMetric
from pypz.core.channels.status import ChannelStatus
from pypz.core.commons.utils import current_time_millis

if TYPE_CHECKING:
    from pypz.core.specs.plugin import InputPortPlugin, OutputPortPlugin


class ChannelReader(ChannelBase):
    """
    This class is the base of the input channel classes.
    The idea is that the class provides some core, protected abstract methods, which shall be implemented by
    the developer. These implementations then will be invoked by invoker methods. This makes sure that additional
    necessary logic will be performed along the implemented.

    :param channel_name: name of the channel
    :param context: the :class:`PortPlugin <pypz.core.specs.plugin.PortPlugin>`, which operates this channel
    :param executor: an external ThreadPoolExecutor, if not provided, on will be created internally
    """

    # ======================= static fields =======================

    NotInitialized = -9999
    """
    Some helper static to init or identify if offset is not initialized
    """

    MetricsBufferLength = 10
    """
    This variable specifies the length of the metric buffer and so the averaging window
    """

    # ======================= Ctor =======================

    def __init__(self, channel_name: str,
                 context: 'InputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._read_record_count: int = 0
        """
        Counter for how many records have been read by this channel. Note that this value can differ from the
        offset value in case of crash-restart. Still the framework maintains the difference between this value
        and the loaded offset and will compensate the framework calculated offset by that difference.
        """

        self._read_record_offset: int = ChannelReader.NotInitialized
        """
        The actual write offset of the read records. Eg. if 10 records have been read and provided to the plugin
        then the offset is 10. Note that this might differ from the m_inputRecordCount_i64 in case of crash-restart.
        """

        self._initial_input_record_offset: int = 0
        """
        This is variable stores the initial offset, which can be different than 0 in case of crash-restart. In this
        case this value is actually the difference by which the framework calculated offset shall be compensated.
        """

        self._last_offset_committed: int = 0
        """
        This value tracks the last committed offset. The main use-case of this variable is to prevent offset commit
        if the new offset equals the last committed.
        """

        self._metrics_buffer: list[ChannelMetric] = list()
        """
        This list holds metric elements, however the only purpose is to be aware of the first
        element to discard like in a circular buffer
        """

        self._aggregated_time_between_reads: int = 0
        """
        Metric value to store the summed up times between reads
        """

        self._aggregated_record_count: int = 0
        """
        Metric value to store the summed up record count
        """

        self._current_read_timestamp: int = 0
        """
        Stores the timestamp of the most current read to be able to calculate elapsed time
        """

        self._current_read_record_count: int = 0
        """
        Stores the record count of the most current read
        """

        self._status_sender_lock = threading.Lock()
        """
        This lock is used to prevent concurrent modification of metrics
        """

    # ======================= Abstract methods =======================

    @abstractmethod
    def _load_input_record_offset(self) -> int:
        """
        This method shall implement the logic to retrieve stored read offset from the underlying technology. This
        can be arbitrary, but the method shall return the stored offset.

        :return: stored offset
        """
        pass

    @abstractmethod
    def has_records(self) -> bool:
        """
        This method shall implement the logic to check, if the reader can still read records.

        :return: True if channel has still records, False if not
        """
        return True

    @abstractmethod
    def _read_records(self) -> list[Any]:
        """
        This method shall implement the logic to read records from the input channel. An ArrayList of records is
        expected.

        :return: list of records OR empty ArrayList if no records read. Null is not accepted.
        """
        pass

    @abstractmethod
    def _commit_offset(self, offset: int) -> None:
        """
        This method shall implement the logic that commits the provided offset using the underlying technology.

        :param offset: provided offset to commit
        """
        pass

    # ======================= Getter/setter/invoker methods =======================

    def get_read_record_count(self) -> int:
        return self._read_record_count

    def get_read_record_offset(self) -> int:
        return self._read_record_offset

    def set_initial_record_offset(self, initial_record_offset: int) -> None:
        """
        This method initializes the internal variables

        :param initial_record_offset: value to be initialized to
        """

        self._initial_input_record_offset = initial_record_offset
        self._last_offset_committed = initial_record_offset
        self._read_record_offset = initial_record_offset

    def set_initial_record_offset_auto(self) -> None:
        """
        This method sets the initial values of the internal variables to the values retrieved by the implementation.
        """

        self.set_initial_record_offset(self._load_input_record_offset())

    def acknowledge_input(self) -> None:
        """
        This method sends the corresponding ack signal to the ChannelWriter
        """

        self.invoke_sync_send_status_message(ChannelStatus.Acknowledged)

    def invoke_read_records(self) -> list[Any]:
        """
        An invoker method that encapsulates the actual implementation. This method
        MUST be called instead of the implemented method directly to ensure proper
        channel functionality.

        :return: list of read records or empty list in case no records read
        """

        read_records = self._read_records()

        read_record_count = len(read_records) if read_records is not None else 0

        if 0 < read_record_count:
            self._read_record_count += read_record_count
            self._read_record_offset += read_record_count
            self._health_check_payload["receivedRecordCount"] = self._read_record_count

        if self._metrics_enabled:
            with self._status_sender_lock:
                if 0 < self._current_read_timestamp:
                    new_metric = ChannelMetric(current_time_millis() - self._current_read_timestamp,
                                               self._current_read_record_count)

                    self._aggregated_time_between_reads += new_metric.elapsedTimeSinceLastIO
                    self._aggregated_record_count += new_metric.recordCountInLastIO

                    self._metrics_buffer.append(new_metric)

                    if ChannelReader.MetricsBufferLength == len(self._metrics_buffer):
                        old_metric = self._metrics_buffer.pop(0)
                        self._aggregated_time_between_reads -= old_metric.elapsedTimeSinceLastIO
                        self._aggregated_record_count -= old_metric.recordCountInLastIO

                self._current_read_timestamp = current_time_millis()
                self._current_read_record_count = read_record_count

        return read_records

    def on_status_message_send(self) -> None:
        if self._metrics_enabled:
            with self._status_sender_lock:
                self._health_check_payload["elapsedTimeSinceLastReadCycleMs"] = 0 if \
                    0 == self._current_read_timestamp else current_time_millis() - self._current_read_timestamp
                self._health_check_payload["averageTimePerReadCycleMs"] = 0 if 0 == len(self._metrics_buffer) else \
                    self._aggregated_time_between_reads / len(self._metrics_buffer)
                self._health_check_payload["averageTimePerReadRecordMs"] = 0 \
                    if 0 == self._aggregated_record_count else \
                    self._aggregated_time_between_reads / self._aggregated_record_count
                self._health_check_payload["averageRecordPerReadCycle"] = 0 if 0 == len(self._metrics_buffer) else \
                    self._aggregated_record_count / len(self._metrics_buffer)

    def invoke_commit_offset(self, offset: int, compensate_with_initial_offset: bool = True) -> None:
        """
        This method is used to invoke the implementation of the abstract method. This is necessary to perform some
        additional actions like compensating the calculated offset with the initial to make sure that the proper
        offset is committed. Note that this compensation makes only sense in case of crash-restart, since the initial
        values are probably not 0. Note that the provided offset is calculated by the calculateApplicableOffsetFrom
        by breaking down the calculated offset by the framework.

        :param offset: offset calculated by the plugin for this channel
        :param compensate_with_initial_offset: if True, the initial offset will be added to the provided offset
        """

        offset_to_commit = offset

        if compensate_with_initial_offset:
            offset_to_commit += self._initial_input_record_offset

            if offset_to_commit > self._read_record_offset:
                raise ValueError(f"Offset is out of range: {offset_to_commit}; "
                                 f"Current read offset? {self._read_record_offset}")

        # TODO - shall it be rather 'offset_to_commit > self._last_offset_committed' ?
        if offset_to_commit != self._last_offset_committed:
            self._commit_offset(offset_to_commit)

            self._last_offset_committed = offset_to_commit

        if offset_to_commit < self._last_offset_committed:
            self._logger.warning(f"!WARNING! Offset to commit ({offset_to_commit}) "
                                 f"is lower than the last committed offset ({self._last_offset_committed}). "
                                 f"Commit ignored.")

    def invoke_commit_current_read_offset(self) -> None:
        """
        This method is used to invoke the implementation of the abstract method.
        """

        self.invoke_commit_offset(self._read_record_offset, False)


class ChannelWriter(ChannelBase):
    """
    This class is the base of the output channel classes.
    The idea is that the class provides some core, protected abstract methods, which shall be implemented by
    the developer. These implementations then will be invoked by invoker methods. This makes sure that additional
    necessary logic will be performed along the implemented.

    :param channel_name: name of the channel
    :param context: the :class:`PortPlugin <pypz.core.specs.plugin.PortPlugin>`, which operates this channel
    :param executor: an external ThreadPoolExecutor, if not provided, on will be created internally
    """

    MetricsBufferLength = 10
    """
    This variable specifies the length of the metric buffer and so the averaging window
    """

    # ======================= Ctor =======================

    def __init__(self, channel_name: str,
                 context: 'OutputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._written_record_count: int = 0
        """
        Number of outputted records.
        """

        self._metrics_buffer: list[ChannelMetric] = list()
        """
        This list holds metric elements, however the only purpose is to be aware of the first
        element to discard like in a circular buffer
        """

        self._aggregated_time_between_outputs: int = 0
        """
        Metric value to store the summed up times between reads
        """

        self._aggregated_record_count: int = 0
        """
        Metric value to store the summed up record count
        """

        self._current_output_timestamp: int = 0
        """
        Stores the timestamp of the most current read to be able to calculate elapsed time
        """

        self._current_output_record_count: int = 0
        """
        Stores the record count of the most current read
        """

        self._status_sender_lock = threading.Lock()
        """
        This lock is used to prevent concurrent modification of metrics
        """

    # ======================= Abstract methods =======================

    @abstractmethod
    def _write_records(self, records: list[Any]) -> None:
        """
        This method shall implement the logic that writes the records to the output resource. It will automatically
        be invoked by the plugin via the corresponding invoker method.

        :param records: list of records to be written
        """
        pass

    # ======================= Getter/setter/invoker methods =======================

    def get_written_record_count(self):
        return self._written_record_count

    def is_all_connected_input_channels_acknowledged(self) -> bool:
        """
        This method retrieves the number of InputChannels that have acknowledged their input (i.e. the output of
        this channel). This information can be useful for scenarios, where a synchronization logic needs to be
        implemented e.g. where the output shall not provide more data until the inputs did not acknowledge. Note
        that the state is maintained entirely by the ChannelWriter i.e. if the ChannelReader sent the proper message,
        a flag will be set in the corresponding entry of the status map. Then this flag will be reset once this
        ChannelWriter produces new data (check invoke_write_records() for more detail).

        :return: number of acknowledged InputChannels
        """

        for status_monitor in list(self._status_map.values()):
            if status_monitor.is_channel_healthy() and \
                    (ChannelStatus.Acknowledged not in status_monitor.status_update_map):
                return False
        return True

    def invoke_write_records(self, records: list[Any]) -> None:
        """
        This method is used to invoke the implementation of the abstract method. This is necessary to perform some
        additional actions e.g. updating number of outputted records.

        :param records: records to be written
        """

        self._write_records(records)

        output_record_count = len(records)

        self._written_record_count += output_record_count

        self._health_check_payload["sentRecordCount"] = self._written_record_count

        # We need to set the acknowledged flag to false, since we output new data to be acknowledged
        for status_monitor in list(self._status_map.values()):
            if ChannelStatus.Acknowledged in status_monitor.status_update_map:
                del status_monitor.status_update_map[ChannelStatus.Acknowledged]

        if self._metrics_enabled:
            with self._status_sender_lock:
                if 0 < self._current_output_timestamp:
                    new_metric = ChannelMetric(current_time_millis() - self._current_output_timestamp,
                                               self._current_output_record_count)

                    self._aggregated_time_between_outputs += new_metric.elapsedTimeSinceLastIO
                    self._aggregated_record_count += new_metric.recordCountInLastIO

                    self._metrics_buffer.append(new_metric)

                    if ChannelWriter.MetricsBufferLength == len(self._metrics_buffer):
                        old_metric = self._metrics_buffer.pop(0)
                        self._aggregated_time_between_outputs -= old_metric.elapsedTimeSinceLastIO
                        self._aggregated_record_count -= old_metric.recordCountInLastIO

                self._current_output_timestamp = current_time_millis()
                self._current_output_record_count = output_record_count

    def on_status_message_send(self):
        if self._metrics_enabled:
            with self._status_sender_lock:
                self._health_check_payload["elapsedTimeSinceLastOutputCycleMs"] = 0 \
                    if 0 == self._current_output_timestamp else current_time_millis() - self._current_output_timestamp
                self._health_check_payload["averageTimePerOutputCycleMs"] = 0 if 0 == len(self._metrics_buffer) else \
                    self._aggregated_time_between_outputs / len(self._metrics_buffer)
                self._health_check_payload["averageTimePerOutputRecordMs"] = 0 \
                    if 0 == self._aggregated_record_count else \
                    self._aggregated_time_between_outputs / self._aggregated_record_count
                self._health_check_payload["averageRecordPerOutputCycle"] = 0 if 0 == len(self._metrics_buffer) else \
                    self._aggregated_record_count / len(self._metrics_buffer)
