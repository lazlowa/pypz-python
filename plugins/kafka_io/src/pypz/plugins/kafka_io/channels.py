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
from typing import Optional, Any, TYPE_CHECKING

from kafka.errors import NoBrokersAvailable, NodeNotReadyError

from pypz.core.channels.io import ChannelWriter, ChannelReader

from kafka.consumer.fetcher import ConsumerRecord
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition, OffsetAndMetadata
from kafka.admin import NewTopic

from avro.io import DatumReader, BinaryDecoder, DatumWriter, BinaryEncoder, AvroTypeException
from avro.schema import parse
import io
from avro_validator.schema import Schema

if TYPE_CHECKING:
    from pypz.core.specs.plugin import InputPortPlugin, OutputPortPlugin


WriterStatusTopicNameExtension = ".output.state"
ReaderStatusTopicNameExtension = ".input.state"
OutputPortConfigStateControlKey = "state"


class KafkaChannelWriter(ChannelWriter):

    # ======================= Static fields =======================

    # ======================= ctor =======================

    def __init__(self, channel_name: str,
                 context: 'OutputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._data_producer: Optional[KafkaProducer] = None
        """
        Data producer, responsible to produce data received by the plugin
        """

        self._writer_status_producer: Optional[KafkaProducer] = None
        """
        Output status producer, responsible to produce control signals and schema
        """

        self._reader_status_consumer: Optional[KafkaConsumer] = None
        """
        Input status consumer for the channel. Reads the status provided by the KafkaInputChannels.
        """

        self._data_producer_properties: dict = dict()
        """
        Property collection for data producer
        """

        self._writer_status_producer_properties: dict = dict()
        """
        Property collection for output status producer
        """

        self._reader_status_consumer_properties: dict = dict()
        """
        Properties for status consumer. This is mainly a copy of the properties of the data consumer. The only
        exception is the group id.
        """

        self._data_topic_name: str = channel_name
        """
        Data topic name
        """

        self._writer_status_topic_name: str = channel_name + WriterStatusTopicNameExtension
        """
        Output status topic name
        """

        self._reader_status_topic_name = channel_name + ReaderStatusTopicNameExtension
        """
        Input status topic name
        """

        self._generic_datum_writer: Optional[DatumWriter] = None
        """
        This is the datum writer that converts generic records to byte data
        """

        self._consumer_timeout_ms: int = 10000
        """
        This member stores the timeout value for the consumer polling.
        """

        self._admin_client: Optional[KafkaAdminClient] = None
        """
        Kafka admin client. Necessary to create/delete topics.
        """

        self._round_robin_partition_idx: int = 0
        """
        This index ensures that we produce in a true round-robin fashion. It is necessary,
        since Kafka's round-robin considers batches of records instead of simple records
        """

        self._target_partition_count: int = 1
        """
        The number of partitions of the topic that is created by the channel reader. This
        value will be read from the Kafka directly.
        """

        # Consumer / producer configuration
        # =================================

        self._data_producer_properties["client_id"] = self._unique_name
        self._data_producer_properties["key_serializer"] = lambda key: key.encode('utf-8') if key else None
        self._data_producer_properties["acks"] = 1
        self._data_producer_properties["retries"] = 10
        self._data_producer_properties["max_block_ms"] = 10000

        self._reader_status_consumer_properties["client_id"] = self._unique_name
        self._reader_status_consumer_properties["key_deserializer"] = lambda key: key.decode("utf-8") if key else ""
        self._reader_status_consumer_properties["value_deserializer"] = lambda val: val.decode("utf-8") \
            if val else "Should not be 'None'. Check for cause!"
        self._reader_status_consumer_properties["enable_auto_commit"] = False
        self._reader_status_consumer_properties["session_timeout_ms"] = 10000
        self._reader_status_consumer_properties["auto_offset_reset"] = "earliest"
        self._reader_status_consumer_properties["group_id"] = self._unique_name + "." + self._reader_status_topic_name

    def set_location(self, channel_location: str):
        super().set_location(channel_location)

        self._data_producer_properties["bootstrap_servers"] = channel_location
        self._reader_status_consumer_properties["bootstrap_servers"] = channel_location

    def can_close(self) -> bool:
        return True

    def _create_resources(self):
        if self._location is None:
            raise AttributeError("Missing channel location parameter.")

        return True

    def _delete_resources(self):
        return True

    def _open_channel(self):
        if self._location is None:
            raise AttributeError("Missing channel location parameter.")

        if self._data_topic_name is None:
            raise AttributeError("Missing channel name.")

        try:
            if self._admin_client is None:
                self._admin_client = KafkaAdminClient(bootstrap_servers=self._location)

            existing_topics = self._admin_client.list_topics()

            if (self._data_topic_name not in existing_topics) or \
                    (self._reader_status_topic_name not in existing_topics) or \
                    (self._writer_status_topic_name not in existing_topics):
                return False

            self._target_partition_count = \
                len(self._admin_client.describe_topics([self._data_topic_name])[0]["partitions"])

            # ===== Initializing producers/consumers =====

            if self._reader_status_consumer is None:
                self._reader_status_consumer = KafkaConsumer(**self._reader_status_consumer_properties)
                self._reader_status_consumer.subscribe(topics=[self._reader_status_topic_name])

            if self._data_producer is None:
                self._data_producer = KafkaProducer(**self._data_producer_properties)

            if self._writer_status_producer is None:
                # Copy all data consumer properties
                self._writer_status_producer_properties = self._data_producer_properties.copy()
                self._writer_status_producer_properties["value_serializer"] = lambda val: val.encode('utf-8')
                self._writer_status_producer = KafkaProducer(**self._writer_status_producer_properties)

            return True
        except (NoBrokersAvailable, NodeNotReadyError) as e:
            # These might be transient errors, if Kafka is overloaded, so retry makes sense
            # TODO - implement timeout mechanism?
            self._logger.warn(e)
            return False

    def _close_channel(self):
        try:
            if self._reader_status_consumer is not None:
                self._reader_status_consumer.close(False)
                self._reader_status_consumer = None

            if self._writer_status_producer is not None:
                self._writer_status_producer.close()
                self._writer_status_producer = None

            if self._data_producer is not None:
                self._data_producer.close()
                self._data_producer = None

            if self._admin_client is not None:
                self._admin_client.close()
                self._resource_checker_admin_client = None

            return True
        except (NoBrokersAvailable, NodeNotReadyError) as e:
            # These might be transient errors, if Kafka is overloaded, so retry makes sense
            # TODO - implement timeout mechanism?
            self._logger.warn(e)
            return False

    def _configure_channel(self, configuration: dict):
        for key in configuration.keys():
            if key in KafkaProducer.DEFAULT_CONFIG:
                self._data_producer_properties[key] = configuration.get(key)

    def _write_records(self, records: list[Any]):
        if not isinstance(records, list):
            raise TypeError(f"Invalid record type: {type(records)}. List of records (dicts) is expected.")

        if self._generic_datum_writer is None:
            self._generic_datum_writer = DatumWriter(writers_schema=parse(self._context.get_schema()))

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
            self._data_producer.send(self._data_topic_name,
                                     key=str(self._round_robin_partition_idx),
                                     value=converted_record,
                                     partition=self._round_robin_partition_idx)

            if 1 < self._target_partition_count:
                self._round_robin_partition_idx += 1

                if self._round_robin_partition_idx == self._target_partition_count:
                    self._round_robin_partition_idx = 0

    def start_channel(self, send_status_message: bool = True):
        super().start_channel(send_status_message)
        self._writer_status_producer.flush()

    def stop_channel(self, send_status_message: bool = True):
        # The reason for that calling order is that we need to ensure
        # that all the data has been flushed before calling the stop.
        # But the stop shall be flushed as well afterward.
        self._data_producer.flush()
        super().stop_channel(send_status_message)
        self._writer_status_producer.flush()

    def _send_status_message(self, status_string):
        self._writer_status_producer.send(self._writer_status_topic_name,
                                          key=OutputPortConfigStateControlKey,
                                          value=status_string)

    def _retrieve_status_messages(self) -> Optional[list]:
        try:
            consumed_state_records = self._reader_status_consumer.poll(self._consumer_timeout_ms)

            status_list = []

            for topic, controlRecords in consumed_state_records.items():
                for controlRecord in controlRecords:
                    if OutputPortConfigStateControlKey in controlRecord.key:
                        status_list.append(controlRecord.value)

            return status_list
        except Exception:
            return None


class KafkaChannelReader(ChannelReader):

    InitialDataConsumerTimeoutInMs = 10000

    DataConsumerTimeoutInMs = 5000

    StatusConsumerTimeoutInMs = 1000

    def __init__(self, channel_name: str,
                 context: 'InputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

        self._data_consumer: Optional[KafkaConsumer] = None
        """
        This is a kafka consumer to poll data from the specified data topic.
        """

        self._writer_status_consumer: Optional[KafkaConsumer] = None
        """
        This is a kafka consumer to poll status updates
        """

        self._reader_status_producer: Optional[KafkaProducer] = None
        """
        Control producer, responsible to produce status messages
        """

        self._data_consumer_properties: dict = dict()
        """
        Properties for data consumer as expected by the KafkaConsumer.
        """

        self._writer_status_consumer_properties: dict = dict()
        """
        Properties for control consumer as expected by the KafkaConsumer. Note that with some extension the complete
        data consumer properties will be copied.
        """

        self._reader_status_producer_properties: dict = dict()
        """
        Properties for status producer. This is mainly a copy of the properties of the data consumer. The only
        exception is the group id.
        """

        self._data_topic_name: str = channel_name
        """
        This member stores the name of the data topic.
        """

        self._writer_status_topic_name: str = channel_name + WriterStatusTopicNameExtension
        """
        This member stores the name of the control topic.
        """

        self._reader_status_topic_name: str = channel_name + ReaderStatusTopicNameExtension
        """
        Input status topic name
        """

        self._consumer_timeout_ms: int = KafkaChannelReader.InitialDataConsumerTimeoutInMs
        """
        This member stores the timeout value for the consumer polling.
        """

        self._generic_datum_reader: Optional[DatumReader] = None
        """
        This is the generic datum reader, which converts bytes to generic records.
        """

        self._target_partition: Optional[TopicPartition] = None
        """
        The topic partition that this channel will read
        """

        self._partition_count: int = 1 if self._context.is_in_group_mode() else self._context.get_group_size()
        """
        The number of partitions to be created for the data channel. If group mode,
        then it shall be 1, since all the channel readers in the group will read all
        the records sent to the channel. Otherwise, it is the size of the group.
        """

        self._admin_client: Optional[KafkaAdminClient] = None
        """
        Kafka admin client. Necessary to check, whether topic is existing on retrieving 0 records on poll.
        """

        # Consumer / producer configuration
        # =================================

        self._data_consumer_properties["client_id"] = self._unique_name
        self._data_consumer_properties["key_deserializer"] = lambda key: key.decode("utf-8") if key else ""
        self._data_consumer_properties["enable_auto_commit"] = False
        # https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout
        self._data_consumer_properties["session_timeout_ms"] = 45 * 1000
        self._data_consumer_properties["max_poll_interval_ms"] = 30 * 60 * 1000
        self._data_consumer_properties["auto_offset_reset"] = "earliest"
        self._data_consumer_properties["max_partition_fetch_bytes"] = 50 * 1024 * 1024
        self._data_consumer_properties["fetch_max_bytes"] = 50 * 1024 * 1024
        self._data_consumer_properties["max_poll_records"] = 1
        self._data_consumer_properties["partition_assignment_strategy"] = [RoundRobinPartitionAssignor]

        self._writer_status_consumer_properties["client_id"] = self._unique_name
        self._writer_status_consumer_properties["group_id"] = self._unique_name + "." + self._writer_status_topic_name
        self._writer_status_consumer_properties["key_deserializer"] = lambda key: key.decode("utf-8") if key else ""
        self._writer_status_consumer_properties["value_deserializer"] = \
            lambda val: val.decode("utf-8") if val else "Should not be 'None'. Check for cause!"
        self._writer_status_consumer_properties["enable_auto_commit"] = False
        # https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout
        self._writer_status_consumer_properties["session_timeout_ms"] = 45 * 1000
        self._writer_status_consumer_properties["max_poll_interval_ms"] = 30 * 60 * 1000
        self._writer_status_consumer_properties["auto_offset_reset"] = "earliest"
        self._writer_status_consumer_properties["max_partition_fetch_bytes"] = 50 * 1024 * 1024
        self._writer_status_consumer_properties["fetch_max_bytes"] = 50 * 1024 * 1024

        self._reader_status_producer_properties["client_id"] = self._unique_name
        self._reader_status_producer_properties["key_serializer"] = lambda key: key.encode('utf-8') if key else None
        self._reader_status_producer_properties["value_serializer"] = lambda key: key.encode('utf-8') \
            if key else "Should not be 'None'. Check for cause!"
        self._reader_status_producer_properties["acks"] = 1
        self._reader_status_producer_properties["retries"] = 10
        self._reader_status_producer_properties["max_block_ms"] = 10000

    def set_location(self, channel_location: str):
        super().set_location(channel_location)

        self._data_consumer_properties["bootstrap_servers"] = channel_location
        self._reader_status_producer_properties["bootstrap_servers"] = channel_location
        self._writer_status_consumer_properties["bootstrap_servers"] = channel_location

    def _open_channel(self):

        if self._location is None:
            raise AttributeError("Missing channel location parameter.")

        try:
            if self._admin_client is None:
                self._admin_client = KafkaAdminClient(bootstrap_servers=self._location)

            existing_topics = self._admin_client.list_topics()

            if (self._data_topic_name not in existing_topics) or \
                    (self._reader_status_topic_name not in existing_topics) or \
                    (self._writer_status_topic_name not in existing_topics):
                return False

            if self._reader_status_producer is None:
                self._reader_status_producer = KafkaProducer(**self._reader_status_producer_properties)

            # To ensure idempotence if it would be necessary
            if self._data_consumer is None:
                if (self._context.get_group_name() is not None) and (not self._context.is_in_group_mode()):
                    self._data_consumer_properties["group_id"] = \
                        self._context.get_group_name() + "." + self._data_topic_name
                else:
                    self._data_consumer_properties["group_id"] = self._unique_name + "." + self._data_topic_name

                self._data_consumer = KafkaConsumer(**self._data_consumer_properties)

                if not self._context.is_in_group_mode():
                    # Assign consumer to partition based on its group index
                    self._target_partition = TopicPartition(self._data_topic_name, self._context.get_group_index())
                else:
                    self._target_partition = TopicPartition(self._data_topic_name, 0)

                self._data_consumer.assign([self._target_partition])

            if self._writer_status_consumer is None:
                self._writer_status_consumer = KafkaConsumer(self._writer_status_topic_name,
                                                             **self._writer_status_consumer_properties)

                topics_to_subscribe = list()
                topics_to_subscribe.append(self._writer_status_topic_name)

                # Subscribe to the reader status topic as well, since the replica
                # states shall be recovered as well in this case.
                if 1 < self._context.get_group_size() and self._context.is_principal():
                    topics_to_subscribe.append(self._reader_status_topic_name)

                self._writer_status_consumer.subscribe(topics=topics_to_subscribe)

            return True
        except (NoBrokersAvailable, NodeNotReadyError) as e:
            # These might be transient errors, if Kafka is overloaded, so retry makes sense
            # TODO - implement timeout mechanism?
            self._logger.warn(e)
            return False

    """
    This implementation of the method will care for client closures.
    """
    def _close_channel(self):
        try:
            if self._reader_status_producer is not None:
                self._reader_status_producer.close()
                self._reader_status_producer = None

            if self._writer_status_consumer is not None:
                self._writer_status_consumer.close(False)
                self._writer_status_consumer = None

            if self._data_consumer is not None:
                self._data_consumer.close(False)
                self._data_consumer = None

            return True
        except (NoBrokersAvailable, NodeNotReadyError) as e:
            # These might be transient errors, if Kafka is overloaded, so retry makes sense
            # TODO - implement timeout mechanism?
            self._logger.warn(e)
            return False

    def _configure_channel(self, configuration: dict):
        for key in configuration.keys():
            if key in KafkaConsumer.DEFAULT_CONFIG:
                self._data_consumer_properties[key] = configuration.get(key)

    def _load_input_record_offset(self) -> int:
        offset_meta_data = self._data_consumer.committed(self._target_partition)
        retrieved_offset = 0

        if offset_meta_data is not None:
            if isinstance(offset_meta_data, OffsetAndMetadata):
                retrieved_offset = offset_meta_data.offset
            else:
                retrieved_offset = offset_meta_data

        self._data_consumer.seek(self._target_partition, retrieved_offset)

        return retrieved_offset

    def _read_records(self):
        if self._generic_datum_reader is None:
            self._generic_datum_reader = DatumReader(parse(self._context.get_schema()))

        output_records = []

        consumed_data_records: dict[str, list[ConsumerRecord]] = \
            self._data_consumer.poll(timeout_ms=self._consumer_timeout_ms)

        if KafkaChannelReader.InitialDataConsumerTimeoutInMs == self._consumer_timeout_ms:
            self._consumer_timeout_ms = KafkaChannelReader.DataConsumerTimeoutInMs

        for topic, records in consumed_data_records.items():
            for record in records:
                decoder = BinaryDecoder(io.BytesIO(record.value))
                output_records.append(self._generic_datum_reader.read(decoder))

        return output_records

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
        return 0 < self.get_consumer_lag()

    def start_channel(self, send_status_message: bool = True):
        super().start_channel(send_status_message)
        self._reader_status_producer.flush()

    def stop_channel(self, send_status_message: bool = True):
        super().stop_channel(send_status_message)
        self._reader_status_producer.flush()

    def _commit_offset(self, offset: int) -> None:
        self._data_consumer.commit({self._target_partition: OffsetAndMetadata(offset, "")})

    def _send_status_message(self, message):
        self._reader_status_producer.send(self._reader_status_topic_name,
                                          key=OutputPortConfigStateControlKey,
                                          value=message)

    def _retrieve_status_messages(self) -> Optional[list]:
        try:
            consumed_state_records = self._writer_status_consumer.poll(KafkaChannelReader.StatusConsumerTimeoutInMs)
            status_list = []

            for topic, controlRecords in consumed_state_records.items():
                for controlRecord in controlRecords:
                    if OutputPortConfigStateControlKey in controlRecord.key:
                        status_list.append(controlRecord.value)

            return status_list
        except Exception:
            return None

    def _create_resources(self):
        if self._location is None:
            raise AttributeError("Missing channel location parameter.")

        try:
            if self._admin_client is None:
                self._admin_client = KafkaAdminClient(bootstrap_servers=self._location)

            # ===== Checking topic existences =====

            existing_topics = self._admin_client.list_topics()

            if self._data_topic_name in existing_topics:
                partitions = self._admin_client.describe_topics([self._data_topic_name])[0]["partitions"]
                if len(partitions) != self._partition_count:
                    self._admin_client.delete_topics(topics=[self._data_topic_name], timeout_ms=30000)
                    existing_topics = self._admin_client.list_topics()

            if self._data_topic_name not in existing_topics:
                self._admin_client.create_topics(new_topics=[NewTopic(self._data_topic_name, self._partition_count, 1)])

            if self._writer_status_topic_name not in existing_topics:
                self._admin_client.create_topics(new_topics=[NewTopic(self._writer_status_topic_name, 1, 1)])

            if self._reader_status_topic_name not in existing_topics:
                self._admin_client.create_topics(new_topics=[NewTopic(self._reader_status_topic_name, 1, 1)])

            return True
        except (NoBrokersAvailable, NodeNotReadyError) as e:
            # These might be transient errors, if Kafka is overloaded, so retry makes sense
            # TODO - implement timeout mechanism?
            self._logger.warn(e)
            return False

    def _delete_resources(self):
        try:
            existing_topics = self._admin_client.list_topics()
            deletable_topics = list()

            if self._data_topic_name in existing_topics:
                deletable_topics.append(self._data_topic_name)

            if self._writer_status_topic_name in existing_topics:
                deletable_topics.append(self._writer_status_topic_name)

            if self._reader_status_topic_name in existing_topics:
                deletable_topics.append(self._reader_status_topic_name)

            if 0 < len(deletable_topics):
                self._admin_client.delete_topics(topics=deletable_topics)

            if self._admin_client is not None:
                self._admin_client.close()
                self._admin_client = None

            return True
        except (NoBrokersAvailable, NodeNotReadyError) as e:
            # These might be transient errors, if Kafka is overloaded, so retry makes sense
            # TODO - implement timeout mechanism?
            self._logger.warn(e)
            return False

    def get_consumer_lag(self) -> int:
        end_offsets = self._data_consumer.end_offsets([self._target_partition])

        overall_lag = 0
        for topic_partition in end_offsets.keys():
            overall_lag += (end_offsets[topic_partition] - self._data_consumer.position(topic_partition))

        return overall_lag
