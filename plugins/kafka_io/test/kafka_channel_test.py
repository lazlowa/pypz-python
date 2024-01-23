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
import unittest

from avro.schema import SchemaParseException
from kafka import KafkaAdminClient, TopicPartition, KafkaConsumer
from kafka.errors import UnknownTopicOrPartitionError

from plugins.kafka_io.test.resources import TestReaderOperator
from pypz.core.specs.misc import BlankInputPortPlugin, BlankOutputPortPlugin
from pypz.plugins.kafka_io.channels import ReaderStatusTopicNameExtension, WriterStatusTopicNameExtension, \
    KafkaChannelReader, KafkaChannelWriter


class KafkaChannelTest(unittest.TestCase):

    bootstrap_url = "localhost:9092"

    test_channel_name = "test_channel"
    test_reader_status_name = test_channel_name + ReaderStatusTopicNameExtension
    test_writer_status_name = test_channel_name + WriterStatusTopicNameExtension

    avro_schema_string = """
    {
        "type": "record",
        "name": "DemoRecord",
        "fields": [
            {
                "name": "demoText",
                "type": "string"
            }
        ]
    }
    """

    test_admin_client: KafkaAdminClient

    @staticmethod
    def generate_records(record_count: int) -> list[dict]:
        generated_records = list()

        for idx in range(record_count):
            generated_records.append({
                "demoText": f"record_{idx}"
            })

        return generated_records

    @classmethod
    def setUpClass(cls) -> None:
        time.sleep(2)

        cls.test_admin_client = KafkaAdminClient(bootstrap_servers=KafkaChannelTest.bootstrap_url)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.test_admin_client.close()

    def tearDown(self) -> None:
        try:
            KafkaChannelTest.test_admin_client.delete_topics([
                KafkaChannelTest.test_channel_name,
                KafkaChannelTest.test_writer_status_name,
                KafkaChannelTest.test_reader_status_name
            ])
        except UnknownTopicOrPartitionError:
            pass

    def test_reader_with_invalid_schema_expect_error(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=""))

        with self.assertRaises(SchemaParseException):
            reader.invoke_read_records()

    def test_writer_with_invalid_schema_expect_error(self):
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=""))

        with self.assertRaises(SchemaParseException):
            writer.invoke_write_records([{}])

    def test_reader_invoke_resource_creation_without_provided_location_expect_error(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))

        with self.assertRaises(AttributeError):
            reader.invoke_resource_creation()

    def test_reader_invoke_resource_creation_expect_resource_created(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))

        reader.set_location(KafkaChannelTest.bootstrap_url)

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertNotIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_reader_status_name, existing_topics)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(reader.is_resource_created())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_reader_status_name, existing_topics)

        KafkaChannelTest.test_admin_client.delete_topics([
            KafkaChannelTest.test_channel_name,
            KafkaChannelTest.test_writer_status_name,
            KafkaChannelTest.test_reader_status_name
        ])

    def test_reader_invoke_resource_creation_with_group_expect_proper_partition_count(self):
        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 12)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.input_port)

        reader.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(reader.is_resource_created())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_reader_status_name, existing_topics)

        topics = KafkaChannelTest.test_admin_client.describe_topics([KafkaChannelTest.test_channel_name])

        self.assertEqual(1, len(topics))
        self.assertEqual(13, len(topics[0]["partitions"]))

        KafkaChannelTest.test_admin_client.delete_topics([
            KafkaChannelTest.test_channel_name,
            KafkaChannelTest.test_writer_status_name,
            KafkaChannelTest.test_reader_status_name
        ])

    def test_reader_invoke_resource_creation_with_group_mode_expect_proper_partition_count(self):
        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 12)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.group_input_port)

        reader.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(reader.is_resource_created())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_reader_status_name, existing_topics)

        topics = KafkaChannelTest.test_admin_client.describe_topics([KafkaChannelTest.test_channel_name])

        self.assertEqual(1, len(topics))
        self.assertEqual(1, len(topics[0]["partitions"]))

        KafkaChannelTest.test_admin_client.delete_topics([
            KafkaChannelTest.test_channel_name,
            KafkaChannelTest.test_writer_status_name,
            KafkaChannelTest.test_reader_status_name
        ])

    def test_reader_invoke_resource_creation_with_group_with_existing_topic_expect_topic_update(self):
        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 1)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.get_replica(0).input_port)

        reader.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(reader.is_resource_created())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_reader_status_name, existing_topics)

        topics = KafkaChannelTest.test_admin_client.describe_topics([KafkaChannelTest.test_channel_name])
        self.assertEqual(2, len(topics[0]["partitions"]))

        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 4)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.get_replica(0).input_port)

        reader.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())

        time.sleep(1)

        self.assertTrue(reader.is_resource_created())

        topics = KafkaChannelTest.test_admin_client.describe_topics([KafkaChannelTest.test_channel_name])
        self.assertEqual(5, len(topics[0]["partitions"]))

        KafkaChannelTest.test_admin_client.delete_topics([
            KafkaChannelTest.test_channel_name,
            KafkaChannelTest.test_writer_status_name,
            KafkaChannelTest.test_reader_status_name
        ])

    def test_reader_invoke_resource_deletion_expect_resource_deleted(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))

        reader.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(reader.is_resource_created())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertIn(KafkaChannelTest.test_reader_status_name, existing_topics)

        self.assertTrue(reader.invoke_resource_deletion())
        self.assertTrue(reader.is_resource_deleted())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertNotIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_reader_status_name, existing_topics)

    def test_reader_invoke_open_channel_expect_consumers_and_subscription_init(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))

        reader.set_location(KafkaChannelTest.bootstrap_url)

        try:
            self.assertTrue(reader.invoke_resource_creation())

            time.sleep(1)

            self.assertTrue(reader.is_resource_created())

            self.assertTrue(reader.invoke_open_channel())

            self.assertIsNotNone(reader._data_consumer)
            self.assertIsNotNone(reader._writer_status_consumer)
            self.assertIsNotNone(reader._reader_status_producer)

            self.assertEqual({TopicPartition(KafkaChannelTest.test_channel_name, 0)},
                             reader._data_consumer.assignment())
            self.assertEqual(reader.get_unique_name() + "." + reader._data_topic_name,
                             reader._data_consumer.config["group_id"])
            self.assertEqual(reader.get_unique_name(), reader._data_consumer.config["client_id"])
            self.assertEqual({TopicPartition(KafkaChannelTest.test_channel_name + WriterStatusTopicNameExtension, 0)},
                             reader._writer_status_consumer.assignment())
            self.assertEqual(reader.get_unique_name() + "." + reader._writer_status_topic_name,
                             reader._writer_status_consumer.config["group_id"])
            self.assertEqual(reader.get_unique_name(), reader._writer_status_consumer.config["client_id"])

            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(reader.invoke_resource_deletion())
        except Exception as e:
            reader.invoke_close_channel()
            reader.invoke_resource_deletion()
            self.fail(e)

    def test_reader_invoke_open_channel_in_group_for_principal_expect_consumers_and_subscription_init(self):
        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 1)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.input_port)

        reader.set_location(KafkaChannelTest.bootstrap_url)

        try:
            self.assertTrue(reader.invoke_resource_creation())

            time.sleep(1)

            self.assertTrue(reader.is_resource_created())

            self.assertTrue(reader.invoke_open_channel())

            self.assertIsNotNone(reader._data_consumer)
            self.assertIsNotNone(reader._writer_status_consumer)
            self.assertIsNotNone(reader._reader_status_producer)

            self.assertEqual({TopicPartition(KafkaChannelTest.test_channel_name, 0)},
                             reader._data_consumer.assignment())
            self.assertEqual(reader.get_context().get_group_name() + "." + reader._data_topic_name,
                             reader._data_consumer.config["group_id"])
            self.assertEqual(reader.get_unique_name(), reader._data_consumer.config["client_id"])
            self.assertEqual({
                TopicPartition(KafkaChannelTest.test_channel_name + WriterStatusTopicNameExtension, 0),
                TopicPartition(KafkaChannelTest.test_channel_name + ReaderStatusTopicNameExtension, 0)
            }, reader._writer_status_consumer.assignment())
            self.assertEqual(reader.get_unique_name() + "." + reader._writer_status_topic_name,
                             reader._writer_status_consumer.config["group_id"])
            self.assertEqual(reader.get_unique_name(), reader._writer_status_consumer.config["client_id"])

            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(reader.invoke_resource_deletion())
        except Exception as e:
            reader.invoke_close_channel()
            reader.invoke_resource_deletion()
            self.fail(e)

    def test_reader_invoke_open_channel_in_group_for_non_principal_expect_consumers_and_subscription_init(self):
        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 1)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.get_replica(0).input_port)

        reader.set_location(KafkaChannelTest.bootstrap_url)

        try:
            self.assertTrue(reader.invoke_resource_creation())

            time.sleep(1)

            self.assertTrue(reader.is_resource_created())

            self.assertTrue(reader.invoke_open_channel())

            self.assertIsNotNone(reader._data_consumer)
            self.assertIsNotNone(reader._writer_status_consumer)
            self.assertIsNotNone(reader._reader_status_producer)

            self.assertEqual({TopicPartition(KafkaChannelTest.test_channel_name, 1)},
                             reader._data_consumer.assignment())
            self.assertEqual(reader.get_context().get_group_name() + "." + reader._data_topic_name,
                             reader._data_consumer.config["group_id"])
            self.assertEqual(reader.get_unique_name(), reader._data_consumer.config["client_id"])
            self.assertEqual({
                TopicPartition(KafkaChannelTest.test_channel_name + WriterStatusTopicNameExtension, 0)
            }, reader._writer_status_consumer.assignment())
            self.assertEqual(reader.get_unique_name() + "." + reader._writer_status_topic_name,
                             reader._writer_status_consumer.config["group_id"])
            self.assertEqual(reader.get_unique_name(), reader._writer_status_consumer.config["client_id"])

            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(reader.invoke_resource_deletion())
        except Exception as e:
            reader.invoke_close_channel()
            reader.invoke_resource_deletion()
            self.fail(e)

    def test_writer_invoke_resource_creation_without_provided_location_expect_error(self):
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        with self.assertRaises(AttributeError):
            writer.invoke_resource_creation()

    def test_writer_invoke_resource_creation_deletion_expect_no_operation(self):
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        self.assertTrue(writer.invoke_resource_deletion())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertNotIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_reader_status_name, existing_topics)

    def test_writer_invoke_open_channel_without_resources_created_expect_unfinished(self):
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        self.assertTrue(writer.invoke_resource_deletion())

        time.sleep(1)

        existing_topics = KafkaChannelTest.test_admin_client.list_topics()
        self.assertNotIn(KafkaChannelTest.test_channel_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_writer_status_name, existing_topics)
        self.assertNotIn(KafkaChannelTest.test_reader_status_name, existing_topics)

        self.assertFalse(writer.invoke_open_channel())

    def test_writer_invoke_open_channel_with_resources_created_expect_all_initialized(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        try:
            self.assertTrue(writer.invoke_open_channel())
            self.assertIsNotNone(writer._reader_status_consumer)

            self.assertEqual({TopicPartition(KafkaChannelTest.test_channel_name + ReaderStatusTopicNameExtension, 0)},
                             writer._reader_status_consumer.assignment())
            self.assertEqual(writer.get_unique_name() + "." + writer._reader_status_topic_name,
                             writer._reader_status_consumer.config["group_id"])
            self.assertEqual(writer.get_unique_name(), writer._reader_status_consumer.config["client_id"])

            self.assertTrue(writer.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())
        except Exception as e:
            writer.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)

    def test_writer_round_robin_record_distribution_without_group(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        test_consumer = KafkaConsumer(
            group_id="test_writer_round_robin_record_distribution_without_group",
            bootstrap_servers=KafkaChannelTest.bootstrap_url
        )

        try:
            self.assertTrue(writer.invoke_open_channel())
            self.assertEqual(1, writer._target_partition_count)

            writer.invoke_write_records(KafkaChannelTest.generate_records(100))

            self.assertEqual(0, writer._round_robin_partition_idx)

            end_offsets = test_consumer.end_offsets([TopicPartition(KafkaChannelTest.test_channel_name, 0)])
            self.assertEqual(100, end_offsets[TopicPartition(KafkaChannelTest.test_channel_name, 0)])

            self.assertTrue(writer.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())
        except Exception as e:
            writer.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)
        finally:
            test_consumer.close()

    def test_writer_round_robin_record_distribution_with_grouped_reader(self):
        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 4)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.input_port)
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        test_consumer = KafkaConsumer(
            group_id="test_writer_round_robin_record_distribution_without_group",
            bootstrap_servers=KafkaChannelTest.bootstrap_url
        )

        try:
            self.assertTrue(writer.invoke_open_channel())
            self.assertEqual(5, writer._target_partition_count)

            topic_partitions = [
                TopicPartition(KafkaChannelTest.test_channel_name, 0),
                TopicPartition(KafkaChannelTest.test_channel_name, 1),
                TopicPartition(KafkaChannelTest.test_channel_name, 2),
                TopicPartition(KafkaChannelTest.test_channel_name, 3),
                TopicPartition(KafkaChannelTest.test_channel_name, 4)
            ]

            writer.invoke_write_records(KafkaChannelTest.generate_records(100))
            time.sleep(1)

            end_offsets = test_consumer.end_offsets(topic_partitions)
            self.assertEqual(20, end_offsets[topic_partitions[0]])
            self.assertEqual(20, end_offsets[topic_partitions[1]])
            self.assertEqual(20, end_offsets[topic_partitions[2]])
            self.assertEqual(20, end_offsets[topic_partitions[3]])
            self.assertEqual(20, end_offsets[topic_partitions[4]])

            writer.invoke_write_records(KafkaChannelTest.generate_records(66))
            time.sleep(1)

            end_offsets = test_consumer.end_offsets(topic_partitions)
            self.assertEqual(34, end_offsets[topic_partitions[0]])
            self.assertEqual(33, end_offsets[topic_partitions[1]])
            self.assertEqual(33, end_offsets[topic_partitions[2]])
            self.assertEqual(33, end_offsets[topic_partitions[3]])
            self.assertEqual(33, end_offsets[topic_partitions[4]])

            self.assertTrue(writer.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())
        except Exception as e:
            writer.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)
        finally:
            test_consumer.close()

    def test_reader_consumer_lag_calculation_in_group(self):
        operator = TestReaderOperator("reader")
        operator.set_parameter("replicationFactor", 1)

        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=operator.input_port)
        reader_1 = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                      context=operator.get_replica(0).input_port)
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.invoke_configure_channel({"max_poll_records": 500})
        reader_1.invoke_configure_channel({"max_poll_records": 500})

        reader.set_location(KafkaChannelTest.bootstrap_url)
        reader_1.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(reader_1.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        test_consumer = KafkaConsumer(
            group_id="test_writer_round_robin_record_distribution_without_group",
            bootstrap_servers=KafkaChannelTest.bootstrap_url
        )

        try:
            self.assertTrue(reader.invoke_open_channel())
            self.assertTrue(reader_1.invoke_open_channel())
            self.assertTrue(writer.invoke_open_channel())

            self.assertEqual(2, writer._target_partition_count)

            topic_partitions = [
                TopicPartition(KafkaChannelTest.test_channel_name, 0),
                TopicPartition(KafkaChannelTest.test_channel_name, 1),
            ]

            writer.invoke_write_records(KafkaChannelTest.generate_records(100))
            time.sleep(1)

            end_offsets = test_consumer.end_offsets(topic_partitions)
            self.assertEqual(50, end_offsets[topic_partitions[0]])
            self.assertEqual(50, end_offsets[topic_partitions[1]])
            self.assertEqual(50, reader.get_consumer_lag())
            self.assertEqual(50, reader_1.get_consumer_lag())

            self.assertEqual(50, len(reader.invoke_read_records()))
            self.assertEqual(50, len(reader_1.invoke_read_records()))

            self.assertEqual(0, reader.get_consumer_lag())
            self.assertEqual(0, reader_1.get_consumer_lag())

            writer.invoke_write_records(KafkaChannelTest.generate_records(50))
            time.sleep(1)

            self.assertEqual(25, reader.get_consumer_lag())
            self.assertEqual(25, reader_1.get_consumer_lag())

            end_offsets = test_consumer.end_offsets(topic_partitions)
            self.assertEqual(75, end_offsets[topic_partitions[0]])
            self.assertEqual(75, end_offsets[topic_partitions[1]])

            self.assertEqual(25, len(reader.invoke_read_records()))
            self.assertEqual(25, len(reader_1.invoke_read_records()))

            self.assertEqual(0, reader.get_consumer_lag())
            self.assertEqual(0, reader_1.get_consumer_lag())

            self.assertTrue(writer.invoke_close_channel())
            self.assertTrue(reader_1.invoke_close_channel())
            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader_1.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())
        except Exception as e:
            writer.invoke_close_channel()
            reader_1.invoke_close_channel()
            reader.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader_1.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)
        finally:
            test_consumer.close()

    def test_reader_without_offset_commit_restart_from_beginning(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.invoke_configure_channel({"max_poll_records": 10})

        reader.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        try:
            self.assertTrue(reader.invoke_open_channel())
            reader.set_initial_record_offset_auto()

            self.assertTrue(writer.invoke_open_channel())

            writer.invoke_write_records(KafkaChannelTest.generate_records(10))
            time.sleep(0.2)

            self.assertEqual(10, len(reader.invoke_read_records()))
            self.assertEqual(10, reader.get_read_record_count())
            self.assertEqual(10, reader.get_read_record_offset())

            self.assertTrue(reader.invoke_close_channel())

            reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                        context=BlankInputPortPlugin("reader",
                                                                     schema=KafkaChannelTest.avro_schema_string))
            reader.set_location(KafkaChannelTest.bootstrap_url)
            reader.invoke_configure_channel({"max_poll_records": 10})

            self.assertTrue(reader.invoke_resource_creation())

            time.sleep(1)

            self.assertTrue(reader.invoke_open_channel())
            reader.set_initial_record_offset_auto()

            self.assertEqual(10, len(reader.invoke_read_records()))
            self.assertEqual(10, reader.get_read_record_count())
            self.assertEqual(10, reader.get_read_record_offset())

            self.assertTrue(writer.invoke_close_channel())
            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())

        except Exception as e:
            writer.invoke_close_channel()
            reader.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)

    def test_reader_with_custom_offset_commit_restart_from_custom_offset(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.invoke_configure_channel({"max_poll_records": 10})

        reader.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        try:
            self.assertTrue(reader.invoke_open_channel())
            reader.set_initial_record_offset_auto()

            self.assertTrue(writer.invoke_open_channel())

            writer.invoke_write_records(KafkaChannelTest.generate_records(10))
            time.sleep(0.2)

            self.assertEqual(10, len(reader.invoke_read_records()))
            self.assertEqual(10, reader.get_read_record_count())
            self.assertEqual(10, reader.get_read_record_offset())

            reader.invoke_commit_offset(5)

            self.assertTrue(reader.invoke_close_channel())

            reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                        context=BlankInputPortPlugin("reader",
                                                                     schema=KafkaChannelTest.avro_schema_string))
            reader.set_location(KafkaChannelTest.bootstrap_url)
            reader.invoke_configure_channel({"max_poll_records": 10})

            self.assertTrue(reader.invoke_resource_creation())

            time.sleep(1)

            self.assertTrue(reader.invoke_open_channel())
            reader.set_initial_record_offset_auto()

            self.assertEqual(5, len(reader.invoke_read_records()))
            self.assertEqual(5, reader.get_read_record_count())
            self.assertEqual(10, reader.get_read_record_offset())

            writer.invoke_write_records(KafkaChannelTest.generate_records(10))
            time.sleep(0.2)

            self.assertEqual(10, len(reader.invoke_read_records()))
            self.assertEqual(15, reader.get_read_record_count())
            self.assertEqual(20, reader.get_read_record_offset())

            self.assertTrue(writer.invoke_close_channel())
            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())

        except Exception as e:
            writer.invoke_close_channel()
            reader.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)

    def test_reader_with_current_read_offset_commit_restart_from_current_offset(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.invoke_configure_channel({"max_poll_records": 10})

        reader.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        try:
            self.assertTrue(reader.invoke_open_channel())
            reader.set_initial_record_offset_auto()

            self.assertTrue(writer.invoke_open_channel())

            writer.invoke_write_records(KafkaChannelTest.generate_records(10))
            time.sleep(5)

            self.assertEqual(10, len(reader.invoke_read_records()))
            self.assertEqual(10, reader.get_read_record_count())
            self.assertEqual(10, reader.get_read_record_offset())

            reader.invoke_commit_current_read_offset()

            self.assertTrue(reader.invoke_close_channel())

            reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                        context=BlankInputPortPlugin("reader",
                                                                     schema=KafkaChannelTest.avro_schema_string))
            reader.set_location(KafkaChannelTest.bootstrap_url)
            reader.invoke_configure_channel({"max_poll_records": 10})

            self.assertTrue(reader.invoke_resource_creation())

            time.sleep(1)

            self.assertTrue(reader.invoke_open_channel())
            reader.set_initial_record_offset_auto()

            self.assertEqual(0, len(reader.invoke_read_records()))
            self.assertEqual(0, reader.get_read_record_count())
            self.assertEqual(10, reader.get_read_record_offset())

            writer.invoke_write_records(KafkaChannelTest.generate_records(10))

            time.sleep(2)

            self.assertTrue(writer.invoke_close_channel())

            self.assertEqual(10, len(reader.invoke_read_records()))
            self.assertEqual(10, reader.get_read_record_count())
            self.assertEqual(20, reader.get_read_record_offset())

            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())

        except Exception as e:
            writer.invoke_close_channel()
            reader.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)

    def test_reader_with_invalid_offset_commit_expect_error(self):
        reader = KafkaChannelReader(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankInputPortPlugin("reader", schema=KafkaChannelTest.avro_schema_string))
        writer = KafkaChannelWriter(channel_name=KafkaChannelTest.test_channel_name,
                                    context=BlankOutputPortPlugin("writer", schema=KafkaChannelTest.avro_schema_string))

        reader.invoke_configure_channel({"max_poll_records": 10})

        reader.set_location(KafkaChannelTest.bootstrap_url)
        writer.set_location(KafkaChannelTest.bootstrap_url)

        self.assertTrue(reader.invoke_resource_creation())
        self.assertTrue(writer.invoke_resource_creation())

        time.sleep(1)

        try:
            self.assertTrue(reader.invoke_open_channel())
            reader.set_initial_record_offset_auto()

            self.assertTrue(writer.invoke_open_channel())

            writer.invoke_write_records(KafkaChannelTest.generate_records(10))
            time.sleep(2)

            with self.assertRaises(ValueError):
                reader.invoke_commit_offset(10)

            self.assertEqual(10, len(reader.invoke_read_records()))
            self.assertEqual(10, reader.get_read_record_count())
            self.assertEqual(10, reader.get_read_record_offset())
            reader.invoke_commit_offset(10)

            with self.assertRaises(ValueError):
                reader.invoke_commit_offset(11)

            self.assertTrue(writer.invoke_close_channel())
            self.assertTrue(reader.invoke_close_channel())
            self.assertTrue(writer.invoke_resource_deletion())
            self.assertTrue(reader.invoke_resource_deletion())

        except Exception as e:
            writer.invoke_close_channel()
            reader.invoke_close_channel()
            writer.invoke_resource_deletion()
            reader.invoke_resource_deletion()
            self.fail(e)
