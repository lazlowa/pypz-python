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
import io
from typing import TYPE_CHECKING, Optional, Any

from amqp import Connection, PreconditionFailed
from avro.io import DatumWriter, BinaryEncoder, AvroTypeException, DatumReader, BinaryDecoder
from avro.schema import parse
from avro_validator import Schema

from pypz.plugins.rmq_io.utils import MessageConsumer, MessageProducer, is_queue_existing, \
    ReaderStatusQueueNameExtension, \
    WriterStatusQueueNameExtension, MaxStatusMessageRetrieveCount, is_exchange_existing
from pypz.core.channels.io import ChannelWriter, ChannelReader

if TYPE_CHECKING:
    from pypz.core.specs.plugin import InputPortPlugin, OutputPortPlugin


class RMQChannelWriter(ChannelWriter):

    def __init__(self, channel_name: str,
                 context: 'OutputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._context: 'OutputPortPlugin' = context

        self._data_exchange_name: str = channel_name
        """
        Data goes through exchanges, a dedicated exchange with this name will be
        created for this channel.
        """

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

        self._config_status_consumer_timeout_sec: float = 0.1
        """
        Configuration parameter to specify the timeout for draining events from the status stream
        """

        self._generic_datum_writer: Optional[DatumWriter] = None
        """
        This is the datum writer that converts generic records to byte data. It
        is only initialized, if schema is provided.
        """

    def _write_records(self, records: list[Any]) -> None:
        if (self._generic_datum_writer is None) and (self._context.get_schema() is not None):
            self._generic_datum_writer = DatumWriter(writers_schema=parse(self._context.get_schema()))

        if self._generic_datum_writer is None:
            """
            If no schema provided, and therefore no Avro will be used,
            then send the records as string.
            """
            for record in records:
                self._data_producer.publish(message=record, exchange_name=self._data_exchange_name)
        else:
            converted_records = list()

            """ Record preparation and sending is separated not to send any record from
                the batch, if some records are not valid """
            for record in records:
                record_bytes = io.BytesIO()
                encoder = BinaryEncoder(record_bytes)

                try:
                    self._generic_datum_writer.write(record, encoder)
                    converted_records.append(record_bytes.getvalue())
                except AvroTypeException:
                    # This line is only executed, if there is an issue with the
                    # data w.r.t. the schema. The used package gives a better message
                    # what is the issue and where
                    self._logger.error(record)
                    schema = Schema(self._context.get_schema())
                    parsed_schema = schema.parse()
                    parsed_schema.validate(record)

            for converted_record in converted_records:
                self._data_producer.publish(message=converted_record, exchange_name=self._data_exchange_name)

    def _create_resources(self) -> bool:
        return True

    def _delete_resources(self) -> bool:
        return True

    def _open_channel(self) -> bool:
        if self.get_location() is None:
            raise ValueError(f"Missing channel location for channel: {self._channel_name}")

        with Connection(host=self.get_location()) as admin_connection:
            admin_channel = admin_connection.channel()
            if not is_queue_existing(self._data_queue_name, admin_channel) or \
                    not is_queue_existing(self._reader_status_stream_name, admin_channel) or \
                    not is_queue_existing(self._writer_status_stream_name, admin_channel) or \
                    not is_exchange_existing(self._data_exchange_name, exchange_type="", channel=admin_channel):
                return False

        if self._reader_status_consumer is None:
            try:
                self._reader_status_consumer = MessageConsumer(
                    consumer_name="reader-status-consumer",
                    max_poll_record=MaxStatusMessageRetrieveCount,
                    host=self.get_location()
                )
                self._reader_status_consumer.subscribe(self._reader_status_stream_name,
                                                       arguments={"x-stream-offset": "first"})
                if (1 < self._context.get_group_size()) and self._context.is_principal():
                    self._reader_status_consumer.subscribe(self._writer_status_stream_name,
                                                           arguments={"x-stream-offset": "first"})
            except PreconditionFailed as e:
                self._reader_status_consumer.close()
                if (406 == e.code) and ("" in e.message):
                    raise ConnectionError("Invalid resource type, stream expected")
                raise

        if self._data_producer is None:
            self._data_producer = MessageProducer(host=self.get_location())

        if self._writer_status_producer is None:
            self._writer_status_producer = MessageProducer(host=self.get_location())

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
        self._writer_status_producer.publish(message=message, queue_name=self._writer_status_stream_name)

    def _retrieve_status_messages(self) -> Optional[list]:
        """
        This implementation retrieves the status messages from the corresponding stream.
        Notice that unlike in case of the queue, where arbitrary amount of messages
        can be received via a drain_events() call, for streams it is always only 1
        message. For this reason, we need to poll all available records at most the
        number of MaxStatusMessageRetrieveCount.
        """

        retrieved_messages = self._reader_status_consumer.poll(self._config_status_consumer_timeout_sec)
        self._reader_status_consumer.commit_messages()
        return retrieved_messages


class RMQChannelReader(ChannelReader):
    def __init__(self, channel_name: str,
                 context: 'InputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._context: 'InputPortPlugin' = context

        self._data_exchange_name: str = channel_name
        """
        Data goes through exchanges, a dedicated exchange with this name will be
        created for this channel.
        """

        self._exchange_type = "direct" if not self._context.is_in_group_mode() else "fanout"
        """
        This controls, how the exchange shall forward messages to the consumers. In group
        mode, all the consumers shall get all the messages, hence we need to specify it as "fanout"
        """

        self._data_queue_name: str = channel_name \
            if (not self._context.is_in_group_mode()) or (self._context.is_principal()) \
            else f"{channel_name}-{self._context.get_group_index()}"
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

        self._config_data_consumer_timeout_sec: float = 0.1
        """
        Configuration parameter to specify the timeout for draining events from the data queue
        """

        self._config_status_consumer_timeout_sec: float = 0.1
        """
        Configuration parameter to specify the timeout for draining events from the status stream
        """

        self._generic_datum_reader: Optional[DatumReader] = None
        """
        This is the generic datum reader, which converts bytes to generic records.
        It will be only initialized, if a schema is provided.
        """

    def _load_input_record_offset(self) -> int:
        """
        Offset has no meaning in queues, nevertheless the value -1 is necessary,
        since if it signalizes that no offset ever was committed.
        """
        return -1

    def can_close(self) -> bool:
        if (not self._context.is_principal()) or (self._context.get_group_name() is None):
            return True

        self.invoke_sync_status_update()

        if 0 == self.retrieve_all_connected_channel_count():
            return True

        finished_replica_count = len(self.retrieve_connected_channel_unique_names(
            lambda flt: (flt.get_channel_group_name() == self._context.get_group_name()) and
                        ((not flt.is_channel_healthy()) or flt.is_channel_stopped() or flt.is_channel_closed())
        ))

        return finished_replica_count == (self._context.get_group_size() - 1)

    def has_records(self) -> bool:
        return self._data_consumer.has_records()

    def _read_records(self) -> list[Any]:
        if (self._generic_datum_reader is None) and (self._context.get_schema() is not None):
            self._generic_datum_reader = DatumReader(parse(self._context.get_schema()))

        if self._generic_datum_reader is None:
            return self._data_consumer.poll(self._config_data_consumer_timeout_sec)
        else:
            converted_records = []

            records = self._data_consumer.poll(self._config_data_consumer_timeout_sec)

            for record in records:
                decoder = BinaryDecoder(io.BytesIO(record))
                converted_records.append(self._generic_datum_reader.read(decoder))

            return converted_records

    def _commit_offset(self, offset: int) -> None:
        self._data_consumer.commit_messages()

    def _create_resources(self) -> bool:
        if self.get_location() is None:
            raise ValueError(f"Missing channel location for channel: {self._channel_name}")

        with (Connection(host=self.get_location()) as admin_connection):
            admin_channel = admin_connection.channel()

            admin_channel.exchange_declare(
                exchange=self._data_exchange_name, type=self._exchange_type,
                passive=False, auto_delete=False, durable=True
            )

            data_queues: list = [self._channel_name]

            if self._context.is_in_group_mode():
                data_queues.extend(
                    [f"{self._channel_name}-{idx}" for idx in range(1, self._context.get_group_size())])

            for data_queue in data_queues:
                admin_channel.queue_declare(
                    data_queue, passive=False, durable=True, exclusive=False, auto_delete=False
                )
                admin_channel.queue_bind(queue=data_queue, exchange=self._data_exchange_name)

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
        if self.get_location() is None:
            raise ValueError(f"Missing channel location for channel: {self._channel_name}")

        with Connection(host=self.get_location()) as admin_connection:
            admin_channel = admin_connection.channel()

            data_queues: list = [self._channel_name]

            if self._context.is_in_group_mode():
                data_queues.extend(
                    [f"{self._channel_name}-{idx}" for idx in range(1, self._context.get_group_size())])

            for data_queue in data_queues:
                """ Note that although pypz has its own flow control, which makes sure that
                    no other channel uses the resources already at this point, glitch can
                    happen, which we shall signalize. The strategy is, if a resource is still
                    in use, then we simply wait for another iteration. In case of data
                    queue, if it is not used, but not empty, then we allow the deletion
                    to conclude, but we display a corresponding error message. """
                if is_queue_existing(queue_name=data_queue, channel=admin_channel):
                    try:
                        admin_channel.queue_delete(queue=data_queue, if_unused=True, if_empty=True)
                    except PreconditionFailed as e:
                        try:
                            # Either used or not empty
                            admin_channel.queue_delete(queue=data_queue, if_unused=True)
                            # Not used, but not empty
                            self._logger.error(f"Queue deleted, but was not empty: {e}")
                        except PreconditionFailed as e2:
                            # Empty, but used
                            self._logger.error(f"Queue cannot be deleted, still used: {e2}")
                            return False

            if is_exchange_existing(exchange_name=self._data_exchange_name,
                                    exchange_type=self._exchange_type,
                                    channel=admin_channel):
                try:
                    admin_channel.exchange_delete(exchange=self._data_exchange_name, if_unused=True)
                except PreconditionFailed as e:
                    self._logger.error(f"Exchange cannot be deleted, still used: {e}")
                    return False

            try:
                if is_queue_existing(self._reader_status_stream_name, admin_channel):
                    admin_channel.queue_delete(self._reader_status_stream_name, if_unused=True)
                if is_queue_existing(self._writer_status_stream_name, admin_channel):
                    admin_channel.queue_delete(self._writer_status_stream_name, if_unused=True)
            except PreconditionFailed as e:
                self._logger.error(f"Resources cannot be deleted, still used: {e}")
                return False

        return True

    def _open_channel(self) -> bool:
        if self.get_location() is None:
            raise ValueError(f"Missing channel location for channel: {self._channel_name}")

        with Connection(host=self.get_location()) as admin_connection:
            admin_channel = admin_connection.channel()
            if not is_queue_existing(self._data_queue_name, admin_channel) or \
                    not is_queue_existing(self._reader_status_stream_name, admin_channel) or \
                    not is_queue_existing(self._writer_status_stream_name, admin_channel):
                return False

        if self._writer_status_consumer is None:
            try:
                self._writer_status_consumer = MessageConsumer(
                    consumer_name="writer-status-consumer",
                    max_poll_record=MaxStatusMessageRetrieveCount,
                    host=self.get_location()
                )
                self._writer_status_consumer.subscribe(self._writer_status_stream_name,
                                                       arguments={"x-stream-offset": "first"})
                if (1 < self._context.get_group_size()) and self._context.is_principal():
                    self._writer_status_consumer.subscribe(self._reader_status_stream_name,
                                                           arguments={"x-stream-offset": "first"})
            except PreconditionFailed as e:
                self._writer_status_consumer.close()
                if (406 == e.code) and ("" in e.message):
                    raise ConnectionError("Invalid resource type, stream expected")
                raise

        """ Notice checking the silent mode. Silent mode is the mode, how sniffer sniffs
            the channels, since the sniffer creates an actual channel. Silent mode prevents
            the sniffer to send status signals or to interfere by any means with the pipeline.
            The issue in this case that, if the sniffer opens a channel and by that registers
            a consumer, the server will send records to this consumer, which obviously not
            intended. Therefore, we check the silent mode here and simply ignore the data
            consumer creation, if set. This way, the sniffer will not have any registered
            data consumer. """
        if (self._data_consumer is None) and (not self._silent_mode):
            self._data_consumer = MessageConsumer(
                consumer_name="data-consumer",
                max_poll_record=self._config_max_poll_records,
                host=self.get_location()
            )
            self._data_consumer.subscribe(self._data_queue_name)

        if self._reader_status_producer is None:
            self._reader_status_producer = MessageProducer(host=self.get_location())

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
        self._reader_status_producer.publish(message=message, queue_name=self._reader_status_stream_name)

    def _retrieve_status_messages(self) -> Optional[list]:
        """
        This implementation retrieves the status messages from the corresponding stream.
        Notice that unlike in case of the queue, where arbitrary amount of messages
        can be received via a drain_events() call, for streams it is always only 1
        message. For this reason, we need to poll all available records at most the
        number of MaxStatusMessageRetrieveCount.
        """

        retrieved_messages = self._writer_status_consumer.poll(self._config_status_consumer_timeout_sec)
        self._writer_status_consumer.commit_messages()
        return retrieved_messages
