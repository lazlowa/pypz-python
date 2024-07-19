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
from typing import TYPE_CHECKING, Optional, Any

from amqp import Connection, PreconditionFailed

from pypz.amqp_io.utils import MessageConsumer, MessageProducer
from pypz.core.channels.io import ChannelWriter, ChannelReader

if TYPE_CHECKING:
    from pypz.core.specs.plugin import InputPortPlugin, OutputPortPlugin

WriterStatusQueueNameExtension = ".writer.status"
ReaderStatusQueueNameExtension = ".reader.status"
MaxStatusMessageRetrieveCount = 100


class AMQPChannelWriter(ChannelWriter):

    def __init__(self, channel_name: str,
                 context: 'OutputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._context: 'OutputPortPlugin' = context

        self._data_queue_name: str = channel_name
        """
        Data queue name
        """

        self._reader_status_stream_name: str = channel_name + ReaderStatusQueueNameExtension
        """
        Name of the stream, which contains the reader status signals
        """

        self._writer_status_stream_name: str = channel_name + WriterStatusQueueNameExtension
        """
        Name of the stream, which contains the writer status signals
        """

        self._data_producer: Optional[MessageProducer] = None
        """
        Producer wrapper to produce data messages for the ChannelReaders
        """

        self._writer_status_producer: Optional[MessageProducer] = None
        """
        Producer wrapper to produce status messages for the ChannelReaders
        """

        self._reader_status_consumer: Optional[MessageConsumer] = None
        """
        Consumer wrapper to consume status messages sent by the ChannelReaders
        """

        self._config_status_consumer_timeout_sec: int = 1
        """
        Configuration parameter to specify the timeout for draining events from the status stream
        """

    def _write_records(self, records: list[Any]) -> None:
        for record in records:
            self._data_producer.publish(record, self._data_queue_name)

    def _create_resources(self) -> bool:
        return True

    def _delete_resources(self) -> bool:
        return True

    def _open_channel(self) -> bool:
        if self._data_queue_name is None:
            raise AttributeError("Missing channel name.")

        if self._reader_status_consumer is None:
            self._reader_status_consumer = MessageConsumer(
                consumer_name="reader-status-consumer",
                max_poll_record=MaxStatusMessageRetrieveCount,
                host=self.get_location()
            )
            self._reader_status_consumer.subscribe(self._reader_status_stream_name)

        if self._data_producer is None:
            self._data_producer = MessageProducer(host=self.get_location())

        if self._writer_status_producer is None:
            self._writer_status_producer = MessageProducer(host=self.get_location())

        if not self._data_producer.is_queue_existing(self._data_queue_name) or \
                not self._reader_status_consumer.is_queue_existing(self._reader_status_stream_name) or \
                not self._writer_status_producer.is_queue_existing(self._writer_status_stream_name):
            return False

        return True

    def can_close(self) -> bool:
        return True

    def _close_channel(self) -> bool:
        if self._data_producer is not None:
            self._data_producer.close()
            self._data_producer = None
        if self._writer_status_producer is not None:
            self._writer_status_producer.close()
            self._writer_status_producer = None
        if self._reader_status_consumer is not None:
            self._reader_status_consumer.close()
            self._reader_status_consumer = None

        return True

    def _configure_channel(self, channel_configuration: dict) -> None:
        if "status_consumer_timeout_sec" in channel_configuration:
            self._config_status_consumer_timeout_sec = channel_configuration["status_consumer_timeout_sec"]

    def _send_status_message(self, message: str) -> None:
        self._writer_status_producer.publish(message, self._writer_status_stream_name)

        if 1 < self._context.get_group_size():
            self._writer_status_producer.publish(message, self._reader_status_stream_name)

    def _retrieve_status_messages(self) -> Optional[list]:
        retrieved_messages = self._reader_status_consumer.poll(self._config_status_consumer_timeout_sec)
        self._reader_status_consumer.commit_messages()
        return retrieved_messages


class AMQPChannelReader(ChannelReader):
    def __init__(self, channel_name: str,
                 context: 'InputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._context: 'InputPortPlugin' = context

        self._data_queue_name: str = channel_name
        """
        Data queue name, where all the data messages go through
        """

        self._reader_status_stream_name: str = channel_name + ReaderStatusQueueNameExtension
        """
        Name of the stream, which contains the reader status signals
        """

        self._writer_status_stream_name: str = channel_name + WriterStatusQueueNameExtension
        """
        Name of the stream, which contains the writer status signals
        """

        self._data_consumer: Optional[MessageConsumer] = None
        """
        Consumer wrapper to consume data messages sent by the ChannelWriter
        """

        self._writer_status_consumer: Optional[MessageConsumer] = None
        """
        Consumer wrapper to consume status messages sent by the ChannelWriters
        """

        self._reader_status_producer: Optional[MessageProducer] = None
        """
        Producer wrapper to produce status messages for the ChannelWriters
        """

        self._config_max_poll_records: int = 100
        """
        Configuration parameter to specify the max number of messages to process in one go
        """

        self._config_data_consumer_timeout_sec: int = 1
        """
        Configuration parameter to specify the timeout for draining events from the data queue
        """

        self._config_status_consumer_timeout_sec: int = 1
        """
        Configuration parameter to specify the timeout for draining events from the status stream
        """

    def _load_input_record_offset(self) -> int:
        """
        Offset has no meaning in queues, nevertheless the value -1 is necessary,
        since if it signalizes that no offset ever was committed.
        """
        return -1

    def has_records(self) -> bool:
        return self._data_consumer.has_records()

    def _read_records(self) -> list[Any]:
        return self._data_consumer.poll(self._config_data_consumer_timeout_sec)

    def _commit_offset(self, offset: int) -> None:
        self._data_consumer.commit_messages()

    def _create_resources(self) -> bool:
        with Connection(host=self.get_location()) as admin_connection:
            admin_channel = admin_connection.channel()

            admin_channel.queue_declare(
                self._data_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False
            )

            admin_channel.queue_declare(
                self._reader_status_stream_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )

            admin_channel.queue_declare(
                self._writer_status_stream_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )

        return True

    def _delete_resources(self) -> bool:
        with Connection(host=self.get_location()) as admin_connection:
            admin_channel = admin_connection.channel()
            admin_channel.queue_delete(self._data_queue_name, if_unused=True, if_empty=True)
            admin_channel.queue_delete(self._reader_status_stream_name, if_unused=True)
            admin_channel.queue_delete(self._writer_status_stream_name, if_unused=True)

        return True

    def _open_channel(self) -> bool:
        if self._data_queue_name is None:
            raise AttributeError("Missing channel name.")

        if self._writer_status_consumer is None:
            self._writer_status_consumer = MessageConsumer(
                consumer_name="writer-status-consumer",
                max_poll_record=MaxStatusMessageRetrieveCount,
                host=self.get_location()
            )
            self._writer_status_consumer.subscribe(self._writer_status_stream_name)

        if self._data_consumer is None:
            self._data_consumer = MessageConsumer(
                consumer_name="data-consumer",
                max_poll_record=self._config_max_poll_records,
                host=self.get_location()
            )
            self._data_consumer.subscribe(self._data_queue_name)

        if self._reader_status_producer is None:
            self._reader_status_producer = MessageProducer(host=self.get_location())

        if not self._data_consumer.is_queue_existing(self._data_queue_name) or \
                not self._reader_status_producer.is_queue_existing(self._reader_status_stream_name) or \
                not self._writer_status_consumer.is_queue_existing(self._writer_status_stream_name):
            return False

        return True

    def _close_channel(self) -> bool:
        if self._data_consumer is not None:
            self._data_consumer.close()
            self._data_consumer = None
        if self._reader_status_producer is not None:
            self._reader_status_producer.close()
            self._reader_status_producer = None
        if self._writer_status_consumer is not None:
            self._writer_status_consumer.close()
            self._writer_status_consumer = None

        return True

    def _configure_channel(self, channel_configuration: dict) -> None:
        if "max_poll_records" in channel_configuration:
            self._config_max_poll_records = channel_configuration["max_poll_records"]

        if "data_consumer_timeout_sec" in channel_configuration:
            self._config_data_consumer_timeout_sec = channel_configuration["data_consumer_timeout_sec"]

        if "status_consumer_timeout_sec" in channel_configuration:
            self._config_status_consumer_timeout_sec = channel_configuration["status_consumer_timeout_sec"]

    def _send_status_message(self, message: str) -> None:
        self._reader_status_producer.publish(message, self._reader_status_stream_name)

        if 1 < self._context.get_group_size():
            self._reader_status_producer.publish(message, self._writer_status_stream_name)

    def _retrieve_status_messages(self) -> Optional[list]:
        retrieved_messages = self._writer_status_consumer.poll(self._config_status_consumer_timeout_sec)
        self._writer_status_consumer.commit_messages()
        return retrieved_messages
