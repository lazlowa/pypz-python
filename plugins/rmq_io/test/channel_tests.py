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
import unittest
from typing import Optional

from amqp import Connection, Message

from plugins.rmq_io.src.pypz.rmq_io.utils import ReaderStatusQueueNameExtension, WriterStatusQueueNameExtension, \
    is_queue_existing, is_exchange_existing
from pypz.core.channels.status import ChannelStatusMessage, ChannelStatus
from pypz.core.specs.misc import BlankOutputPortPlugin, BlankInputPortPlugin, BlankOperator
from pypz.rmq_io.channels import RMQChannelWriter, RMQChannelReader


class RMQChannelTest(unittest.TestCase):
    bootstrap_url = "localhost:5672"

    test_channel_name = "test_channel"
    test_reader_status_queue_name = test_channel_name + ReaderStatusQueueNameExtension
    test_writer_status_queue_name = test_channel_name + WriterStatusQueueNameExtension

    connection: Optional[Connection] = None

    @staticmethod
    def generate_records(record_count: int) -> list[dict]:
        generated_records = list()

        for idx in range(record_count):
            generated_records.append({
                "demoText": f"record_{idx}"
            })

        return generated_records

    @classmethod
    def setUpClass(cls):
        cls.connection = Connection(host=RMQChannelTest.bootstrap_url)
        cls.connection.connect()
        cls.admin_channel = cls.connection.channel()

    @classmethod
    def tearDownClass(cls):
        # Close the connection after all tests are done
        cls.connection.close()

    def setUp(self):
        print(f"Start: {self._testMethodName}")

    def tearDown(self) -> None:
        print(f"Cleanup: {self._testMethodName}")
        self.admin_channel.queue_delete(RMQChannelTest.test_channel_name)
        self.admin_channel.queue_delete(RMQChannelTest.test_reader_status_queue_name)
        self.admin_channel.queue_delete(RMQChannelTest.test_writer_status_queue_name)
        self.admin_channel.exchange_delete(RMQChannelTest.test_channel_name)
        print(f"End: {self._testMethodName}")

    def test_channel_writer_open_without_resources_expect_nok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        self.assertFalse(channel_writer.invoke_open_channel())

        self.admin_channel.queue_declare(
            RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
        )
        self.assertFalse(channel_writer.invoke_open_channel())

        self.admin_channel.queue_declare(
            RMQChannelTest.test_reader_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )
        self.assertFalse(channel_writer.invoke_open_channel())

        self.admin_channel.queue_declare(
            RMQChannelTest.test_writer_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )
        self.assertFalse(channel_writer.invoke_open_channel())
        self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_open_without_location_expect_error(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        with self.assertRaises(ValueError):
            channel_writer.invoke_open_channel()

    def test_channel_writer_open_close_with_resources_expect_ok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        self.admin_channel.queue_declare(
            RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_reader_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_writer_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )
        self.admin_channel.exchange_declare(
            exchange=RMQChannelTest.test_channel_name, type="direct",
            passive=False, auto_delete=False, durable=True
        )
        self.admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

        try:
            self.assertTrue(channel_writer.invoke_open_channel())
        finally:
            self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_open_close_with_invalid_resources_expect_error(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        self.admin_channel.queue_declare(
            RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
        )
        self.assertFalse(channel_writer.invoke_open_channel())

        self.admin_channel.queue_declare(
            RMQChannelTest.test_reader_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False,
        )
        self.assertFalse(channel_writer.invoke_open_channel())

        self.admin_channel.queue_declare(
            RMQChannelTest.test_writer_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False,
        )
        self.assertFalse(channel_writer.invoke_open_channel())

        self.admin_channel.exchange_declare(
            exchange=RMQChannelTest.test_channel_name, type="direct",
            passive=False, auto_delete=False, durable=True
        )
        self.admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

        with self.assertRaises(ConnectionError):
            self.assertTrue(channel_writer.invoke_open_channel())

        self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_publish_messages_expect_ok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        self.admin_channel.queue_declare(
            RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_reader_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_writer_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )

        self.admin_channel.exchange_declare(
            exchange=RMQChannelTest.test_channel_name, type="direct",
            passive=False, auto_delete=False, durable=True
        )
        self.admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

        try:
            self.assertTrue(channel_writer.invoke_open_channel())

            test_messages = ["record_0", "record_1", "record_2"]
            channel_writer.invoke_write_records(test_messages)

            for message in test_messages:
                pushed_message = self.admin_channel.basic_get(RMQChannelTest.test_channel_name)
                self.assertIsNotNone(pushed_message)
                self.assertEqual(message, pushed_message.body)

        finally:
            self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_retrieve_status_message_with_single_connected_channel_expect_ok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        self.admin_channel.queue_declare(
            RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_reader_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_writer_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )

        self.admin_channel.exchange_declare(
            exchange=RMQChannelTest.test_channel_name, type="direct",
            passive=False, auto_delete=False, durable=True
        )
        self.admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

        try:
            self.assertTrue(channel_writer.invoke_open_channel())

            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name="reader",
                        status=ChannelStatus.Opened
                    )
                )), routing_key=RMQChannelTest.test_reader_status_queue_name)

            channel_writer.invoke_sync_status_update()

            self.assertEqual(1, channel_writer.retrieve_all_connected_channel_count())
            connected_open_channel_count = channel_writer.retrieve_connected_channel_unique_names(
                lambda flt: flt.is_channel_opened()
            )
            self.assertEqual(1, len(connected_open_channel_count))
            self.assertEqual(f"{RMQChannelTest.test_channel_name}@reader", connected_open_channel_count.pop())

            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name="reader",
                        status=ChannelStatus.Closed
                    )
                )), routing_key=RMQChannelTest.test_reader_status_queue_name)

            channel_writer.invoke_sync_status_update()

            self.assertEqual(1, channel_writer.retrieve_all_connected_channel_count())
            connected_open_channel_count = channel_writer.retrieve_connected_channel_unique_names(
                lambda flt: flt.is_channel_opened()
            )
            connected_closed_channel_count = channel_writer.retrieve_connected_channel_unique_names(
                lambda flt: flt.is_channel_closed()
            )
            self.assertEqual(0, len(connected_open_channel_count))
            self.assertEqual(1, len(connected_closed_channel_count))
            self.assertEqual(f"{RMQChannelTest.test_channel_name}@reader", connected_closed_channel_count.pop())
        finally:
            self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_retrieve_status_message_with_multi_connected_channel_expect_ok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        self.admin_channel.queue_declare(
            RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_reader_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )
        self.admin_channel.queue_declare(
            RMQChannelTest.test_writer_status_queue_name,
            passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
        )

        self.admin_channel.exchange_declare(
            exchange=RMQChannelTest.test_channel_name, type="direct",
            passive=False, auto_delete=False, durable=True
        )
        self.admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

        try:
            self.assertTrue(channel_writer.invoke_open_channel())

            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name="reader",
                        status=ChannelStatus.Opened
                    )
                )), routing_key=RMQChannelTest.test_reader_status_queue_name)

            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name="reader_0",
                        status=ChannelStatus.Opened
                    )
                )), routing_key=RMQChannelTest.test_reader_status_queue_name)

            channel_writer.invoke_sync_status_update()

            self.assertEqual(2, channel_writer.retrieve_all_connected_channel_count())
            connected_open_channel_count = channel_writer.retrieve_connected_channel_unique_names(
                lambda flt: flt.is_channel_opened()
            )
            self.assertEqual(2, len(connected_open_channel_count))
            self.assertIn(f"{RMQChannelTest.test_channel_name}@reader", connected_open_channel_count)
            self.assertIn(f"{RMQChannelTest.test_channel_name}@reader_0", connected_open_channel_count)
        finally:
            self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_reader_resource_creation_without_location_expect_error(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        with self.assertRaises(ValueError):
            channel_reader.invoke_resource_creation()

    def test_channel_reader_resource_creation_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())

        self.assertTrue(is_queue_existing(RMQChannelTest.test_channel_name, self.admin_channel))
        self.assertTrue(is_queue_existing(RMQChannelTest.test_reader_status_queue_name, self.admin_channel))
        self.assertTrue(is_queue_existing(RMQChannelTest.test_writer_status_queue_name, self.admin_channel))
        self.assertTrue(is_exchange_existing(RMQChannelTest.test_channel_name, "", self.admin_channel))

    def test_channel_reader_resource_creation_in_group_mode_expect_ok(self):
        operator_context = BlankOperator("blank")
        operator_context.set_parameter("replicationFactor", 2)
        plugin_context = BlankInputPortPlugin("reader", group_mode=True, context=operator_context)
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=plugin_context)
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())

        try:
            self.assertTrue(is_queue_existing(RMQChannelTest.test_channel_name, self.admin_channel))
            self.assertTrue(is_queue_existing(RMQChannelTest.test_channel_name + "-1", self.admin_channel))
            self.assertTrue(is_queue_existing(RMQChannelTest.test_channel_name + "-2", self.admin_channel))
            self.assertTrue(is_queue_existing(RMQChannelTest.test_reader_status_queue_name, self.admin_channel))
            self.assertTrue(is_queue_existing(RMQChannelTest.test_writer_status_queue_name, self.admin_channel))
            self.assertTrue(is_exchange_existing(RMQChannelTest.test_channel_name, "", self.admin_channel))
        finally:
            self.admin_channel.queue_delete(RMQChannelTest.test_channel_name + "-1")
            self.admin_channel.queue_delete(RMQChannelTest.test_channel_name + "-2")

    def test_channel_reader_resource_creation_idempotence(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())
        self.assertTrue(channel_reader.invoke_resource_creation())

    def test_channel_reader_resource_deletion_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())
        self.assertTrue(channel_reader.invoke_resource_deletion())

        self.assertFalse(is_queue_existing(RMQChannelTest.test_channel_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_reader_status_queue_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_writer_status_queue_name, self.admin_channel))
        self.assertFalse(is_exchange_existing(RMQChannelTest.test_channel_name, "", self.admin_channel))

    def test_channel_reader_resource_deletion_in_group_mode_expect_ok(self):
        operator_context = BlankOperator("blank")
        operator_context.set_parameter("replicationFactor", 2)
        plugin_context = BlankInputPortPlugin("reader", group_mode=True, context=operator_context)
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=plugin_context)
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())
        self.assertTrue(channel_reader.invoke_resource_deletion())

        self.assertFalse(is_queue_existing(RMQChannelTest.test_channel_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_channel_name + "-1", self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_channel_name + "-2", self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_reader_status_queue_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_writer_status_queue_name, self.admin_channel))
        self.assertFalse(is_exchange_existing(RMQChannelTest.test_channel_name, "", self.admin_channel))

    def test_channel_reader_resource_deletion_with_active_consumer_expect_nok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())

        self.admin_channel.basic_consume(RMQChannelTest.test_channel_name,
                                    callback=lambda: None, consumer_tag="test-consumer")
        self.assertFalse(channel_reader.invoke_resource_deletion())
        self.assertTrue(is_queue_existing(RMQChannelTest.test_channel_name, self.admin_channel))
        self.assertTrue(is_queue_existing(RMQChannelTest.test_reader_status_queue_name, self.admin_channel))
        self.assertTrue(is_queue_existing(RMQChannelTest.test_writer_status_queue_name, self.admin_channel))
        self.assertTrue(is_exchange_existing(RMQChannelTest.test_channel_name, "", self.admin_channel))

        self.admin_channel.basic_cancel(consumer_tag="test-consumer")
        self.assertTrue(channel_reader.invoke_resource_deletion())
        self.assertFalse(is_queue_existing(RMQChannelTest.test_channel_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_reader_status_queue_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_writer_status_queue_name, self.admin_channel))
        self.assertFalse(is_exchange_existing(RMQChannelTest.test_channel_name, "", self.admin_channel))

    def test_channel_reader_resource_deletion_if_non_empty_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())

        self.admin_channel.basic_publish(Message(str("dummy")), routing_key=RMQChannelTest.test_channel_name)
        self.assertTrue(channel_reader.invoke_resource_deletion())
        self.assertFalse(is_queue_existing(RMQChannelTest.test_channel_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_reader_status_queue_name, self.admin_channel))
        self.assertFalse(is_queue_existing(RMQChannelTest.test_writer_status_queue_name, self.admin_channel))
        self.assertFalse(is_exchange_existing(RMQChannelTest.test_channel_name, "", self.admin_channel))

    def test_channel_reader_resource_deletion_idempotence(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())
        self.assertTrue(channel_reader.invoke_resource_deletion())
        self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_open_without_resources_expect_nok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertFalse(channel_reader.invoke_open_channel())

    def test_channel_reader_open_without_location_expect_error(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        with self.assertRaises(ValueError):
            channel_reader.invoke_open_channel()

    def test_channel_reader_open_close_with_resources_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())
        self.assertTrue(channel_reader.invoke_open_channel())
        self.assertTrue(channel_reader.invoke_close_channel())
        self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_open_close_idempotence(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        self.assertTrue(channel_reader.invoke_resource_creation())
        self.assertTrue(channel_reader.invoke_open_channel())
        self.assertTrue(channel_reader.invoke_open_channel())
        self.assertTrue(channel_reader.invoke_close_channel())
        self.assertTrue(channel_reader.invoke_close_channel())
        self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_retrieve_without_commit_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(0, len(retrieved))
            self.admin_channel.basic_publish(Message(str("dummy_0")), routing_key=RMQChannelTest.test_channel_name)
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(1, len(retrieved))
            self.assertEqual("dummy_0", retrieved[0])
            self.admin_channel.basic_publish(Message(str("dummy_1")), routing_key=RMQChannelTest.test_channel_name)
            self.admin_channel.basic_publish(Message(str("dummy_2")), routing_key=RMQChannelTest.test_channel_name)
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual("dummy_1", retrieved[0])
            self.assertEqual("dummy_2", retrieved[1])
            self.assertEqual(2, len(retrieved))
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertEqual(3, self.admin_channel.queue_declare(RMQChannelTest.test_channel_name,
                                                            passive=True).message_count)
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_retrieve_with_commit_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(0, len(retrieved))
            self.admin_channel.basic_publish(Message(str("dummy_0")), routing_key=RMQChannelTest.test_channel_name)
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(1, len(retrieved))
            self.assertEqual("dummy_0", retrieved[0])
            self.admin_channel.basic_publish(Message(str("dummy_1")), routing_key=RMQChannelTest.test_channel_name)
            self.admin_channel.basic_publish(Message(str("dummy_2")), routing_key=RMQChannelTest.test_channel_name)
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual("dummy_1", retrieved[0])
            self.assertEqual("dummy_2", retrieved[1])
            self.assertEqual(2, len(retrieved))
            channel_reader.invoke_commit_current_read_offset()
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertEqual(0, self.admin_channel.queue_declare(RMQChannelTest.test_channel_name,
                                                            passive=True).message_count)
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_has_records_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())
            self.assertFalse(channel_reader.has_records())
            for idx in range(200):
                self.admin_channel.basic_publish(Message(str(f"dummy_{idx}")),
                                            routing_key=RMQChannelTest.test_channel_name)
            self.assertTrue(channel_reader.has_records())
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_can_close_without_replication_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())
            self.assertTrue(channel_reader.can_close())
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_can_close_with_replication_as_principal_with_connection_expect_ok(self):
        operator_context = BlankOperator("blank")
        operator_context.__setattr__("reader", BlankInputPortPlugin("reader", context=operator_context))
        operator_context.set_parameter("replicationFactor", 1)
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=operator_context.reader)
        channel_reader.set_location(RMQChannelTest.bootstrap_url)

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())

            # With no connected replica channel
            self.assertTrue(channel_reader.can_close())

            # With connected replica channel
            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name=operator_context.get_replica(0).reader.get_full_name(),
                        channel_group_name=operator_context.get_replica(0).reader.get_group_name(),
                        channel_group_index=operator_context.get_replica(0).reader.get_group_index(),
                        status=ChannelStatus.Opened
                    )
                )), routing_key=RMQChannelTest.test_writer_status_queue_name)
            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name=operator_context.get_replica(0).reader.get_full_name(),
                        channel_group_name=operator_context.get_replica(0).reader.get_group_name(),
                        channel_group_index=operator_context.get_replica(0).reader.get_group_index(),
                        status=ChannelStatus.HealthCheck
                    )
                )), routing_key=RMQChannelTest.test_writer_status_queue_name)
            self.assertFalse(channel_reader.can_close())

            # With connected and closed channel
            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name=operator_context.get_replica(0).reader.get_full_name(),
                        channel_group_name=operator_context.get_replica(0).reader.get_group_name(),
                        channel_group_index=operator_context.get_replica(0).reader.get_group_index(),
                        status=ChannelStatus.Closed
                    )
                )), routing_key=RMQChannelTest.test_writer_status_queue_name)
            self.assertTrue(channel_reader.can_close())
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_can_close_with_replication_as_replica_with_connection_expect_ok(self):
        operator_context = BlankOperator("blank")
        operator_context.__setattr__("reader", BlankInputPortPlugin("reader", context=operator_context))
        operator_context.set_parameter("replicationFactor", 2)
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=operator_context.get_replica(1).reader)
        channel_reader.set_location(RMQChannelTest.bootstrap_url)

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())

            # With no connected replica channel
            self.assertTrue(channel_reader.can_close())

            # With connected replica channel
            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name=operator_context.get_replica(0).reader.get_full_name(),
                        channel_group_name=operator_context.get_replica(0).reader.get_group_name(),
                        channel_group_index=operator_context.get_replica(0).reader.get_group_index(),
                        status=ChannelStatus.Opened
                    )
                )), routing_key=RMQChannelTest.test_writer_status_queue_name)
            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name=operator_context.get_replica(0).reader.get_full_name(),
                        channel_group_name=operator_context.get_replica(0).reader.get_group_name(),
                        channel_group_index=operator_context.get_replica(0).reader.get_group_index(),
                        status=ChannelStatus.HealthCheck
                    )
                )), routing_key=RMQChannelTest.test_writer_status_queue_name)
            self.assertTrue(channel_reader.can_close())

            # With connected and closed channel
            self.admin_channel.basic_publish(Message(
                str(
                    ChannelStatusMessage(
                        channel_name=RMQChannelTest.test_channel_name,
                        channel_context_name=operator_context.get_replica(0).reader.get_full_name(),
                        channel_group_name=operator_context.get_replica(0).reader.get_group_name(),
                        channel_group_index=operator_context.get_replica(0).reader.get_group_index(),
                        status=ChannelStatus.Closed
                    )
                )), routing_key=RMQChannelTest.test_writer_status_queue_name)
            self.assertTrue(channel_reader.can_close())
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_max_poll_record_1_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        channel_reader.invoke_configure_channel({"max_poll_records": 1})

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())
            for idx in range(100):
                self.admin_channel.basic_publish(Message(str(f"dummy_{idx}")),
                                            routing_key=RMQChannelTest.test_channel_name)
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(1, len(retrieved))
            channel_reader.invoke_commit_current_read_offset()
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(1, len(retrieved))
            channel_reader.invoke_commit_current_read_offset()
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_max_poll_record_27_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        channel_reader.invoke_configure_channel({"max_poll_records": 27})

        try:
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())
            for idx in range(100):
                self.admin_channel.basic_publish(Message(str(f"dummy_{idx}")),
                                            routing_key=RMQChannelTest.test_channel_name)
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(27, len(retrieved))
            channel_reader.invoke_commit_current_read_offset()
            retrieved = channel_reader.invoke_read_records()
            self.assertEqual(27, len(retrieved))
            channel_reader.invoke_commit_current_read_offset()
        finally:
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertTrue(channel_reader.invoke_resource_deletion())

    def test_channel_reader_writer_connection_expect_ok(self):
        channel_reader = RMQChannelReader(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankInputPortPlugin("reader"))
        channel_reader.set_location(RMQChannelTest.bootstrap_url)
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        try:
            self.assertTrue(channel_writer.invoke_resource_creation())
            self.assertFalse(channel_writer.invoke_open_channel())
            self.assertTrue(channel_reader.invoke_resource_creation())
            self.assertTrue(channel_reader.invoke_open_channel())
            self.assertTrue(channel_writer.invoke_resource_creation())
            self.assertTrue(channel_writer.invoke_open_channel())

            channel_writer.invoke_write_records(["dummy_0", "dummy_1"])
            retrieved = channel_reader.invoke_read_records()
            channel_reader.invoke_commit_current_read_offset()
            self.assertEqual(2, len(retrieved))
            self.assertEqual("dummy_0", retrieved[0])
            self.assertEqual("dummy_1", retrieved[1])
        finally:
            self.assertTrue(channel_writer.invoke_close_channel())
            self.assertTrue(channel_writer.invoke_resource_deletion())
            self.assertTrue(channel_reader.invoke_close_channel())
            self.assertTrue(channel_reader.invoke_resource_deletion())
