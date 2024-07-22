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

from amqp import Connection, Message

from plugins.rmq_io.src.pypz.rmq_io.utils import ReaderStatusQueueNameExtension, WriterStatusQueueNameExtension
from pypz.core.channels.status import ChannelStatusMessage, ChannelStatus
from pypz.core.specs.misc import BlankOutputPortPlugin, BlankInputPortPlugin
from pypz.rmq_io.channels import RMQChannelWriter


class RMQChannelTest(unittest.TestCase):
    bootstrap_url = "localhost:5672"

    test_channel_name = "test_channel"
    test_reader_status_queue_name = test_channel_name + ReaderStatusQueueNameExtension
    test_writer_status_queue_name = test_channel_name + WriterStatusQueueNameExtension

    @staticmethod
    def generate_records(record_count: int) -> list[dict]:
        generated_records = list()

        for idx in range(record_count):
            generated_records.append({
                "demoText": f"record_{idx}"
            })

        return generated_records

    def tearDown(self) -> None:
        with Connection(host=RMQChannelTest.bootstrap_url) as admin_connection:
            admin_channel = admin_connection.channel()
            admin_channel.queue_delete(RMQChannelTest.test_channel_name)
            admin_channel.queue_delete(RMQChannelTest.test_reader_status_queue_name)
            admin_channel.queue_delete(RMQChannelTest.test_writer_status_queue_name)
            admin_channel.exchange_delete(RMQChannelTest.test_channel_name)

    def test_channel_writer_open_without_resources_expect_nok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)
        self.assertFalse(channel_writer.invoke_open_channel())

    def test_channel_writer_open_without_location_expect_error(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        with self.assertRaises(ValueError):
            channel_writer.invoke_open_channel()

    def test_channel_writer_open_close_with_resources_expect_ok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        with Connection(host=RMQChannelTest.bootstrap_url) as admin_connection:
            admin_channel = admin_connection.channel()

            admin_channel.queue_declare(
                RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
            )
            self.assertFalse(channel_writer.invoke_open_channel())

            admin_channel.queue_declare(
                RMQChannelTest.test_reader_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )
            self.assertFalse(channel_writer.invoke_open_channel())

            admin_channel.queue_declare(
                RMQChannelTest.test_writer_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )
            self.assertFalse(channel_writer.invoke_open_channel())

            admin_channel.exchange_declare(
                exchange=RMQChannelTest.test_channel_name, type="direct",
                passive=False, auto_delete=False, durable=True
            )
            admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

            try:
                self.assertTrue(channel_writer.invoke_open_channel())
            finally:
                self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_open_close_with_invalid_resources_expect_error(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        with Connection(host=RMQChannelTest.bootstrap_url) as admin_connection:
            admin_channel = admin_connection.channel()

            admin_channel.queue_declare(
                RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
            )
            self.assertFalse(channel_writer.invoke_open_channel())

            admin_channel.queue_declare(
                RMQChannelTest.test_reader_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False,
            )
            self.assertFalse(channel_writer.invoke_open_channel())

            admin_channel.queue_declare(
                RMQChannelTest.test_writer_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False,
            )
            self.assertFalse(channel_writer.invoke_open_channel())

            admin_channel.exchange_declare(
                exchange=RMQChannelTest.test_channel_name, type="direct",
                passive=False, auto_delete=False, durable=True
            )
            admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

            with self.assertRaises(ConnectionError):
                self.assertTrue(channel_writer.invoke_open_channel())

            self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_publish_messages_expect_ok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        with Connection(host=RMQChannelTest.bootstrap_url) as admin_connection:
            admin_channel = admin_connection.channel()

            admin_channel.queue_declare(
                RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
            )
            admin_channel.queue_declare(
                RMQChannelTest.test_reader_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )
            admin_channel.queue_declare(
                RMQChannelTest.test_writer_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )

            admin_channel.exchange_declare(
                exchange=RMQChannelTest.test_channel_name, type="direct",
                passive=False, auto_delete=False, durable=True
            )
            admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

            try:
                self.assertTrue(channel_writer.invoke_open_channel())

                test_messages = ["record_0", "record_1", "record_2"]
                channel_writer.invoke_write_records(test_messages)

                for message in test_messages:
                    pushed_message = admin_channel.basic_get(RMQChannelTest.test_channel_name)
                    self.assertIsNotNone(pushed_message)
                    self.assertEqual(message, pushed_message.body)

            finally:
                self.assertTrue(channel_writer.invoke_close_channel())

    def test_channel_writer_retrieve_status_message_with_single_connected_channel_expect_ok(self):
        channel_writer = RMQChannelWriter(channel_name=RMQChannelTest.test_channel_name,
                                          context=BlankOutputPortPlugin("writer"))
        channel_writer.set_location(RMQChannelTest.bootstrap_url)

        with Connection(host=RMQChannelTest.bootstrap_url) as admin_connection:
            admin_channel = admin_connection.channel()

            admin_channel.queue_declare(
                RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
            )
            admin_channel.queue_declare(
                RMQChannelTest.test_reader_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )
            admin_channel.queue_declare(
                RMQChannelTest.test_writer_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )

            admin_channel.exchange_declare(
                exchange=RMQChannelTest.test_channel_name, type="direct",
                passive=False, auto_delete=False, durable=True
            )
            admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

            try:
                self.assertTrue(channel_writer.invoke_open_channel())

                admin_channel.basic_publish(Message(
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

                admin_channel.basic_publish(Message(
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

        with Connection(host=RMQChannelTest.bootstrap_url) as admin_connection:
            admin_channel = admin_connection.channel()

            admin_channel.queue_declare(
                RMQChannelTest.test_channel_name, passive=False, durable=True, exclusive=False, auto_delete=False
            )
            admin_channel.queue_declare(
                RMQChannelTest.test_reader_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )
            admin_channel.queue_declare(
                RMQChannelTest.test_writer_status_queue_name,
                passive=False, durable=True, exclusive=False, auto_delete=False, arguments={"x-queue-type": "stream"}
            )

            admin_channel.exchange_declare(
                exchange=RMQChannelTest.test_channel_name, type="direct",
                passive=False, auto_delete=False, durable=True
            )
            admin_channel.queue_bind(queue=RMQChannelTest.test_channel_name, exchange=RMQChannelTest.test_channel_name)

            try:
                self.assertTrue(channel_writer.invoke_open_channel())

                admin_channel.basic_publish(Message(
                    str(
                        ChannelStatusMessage(
                            channel_name=RMQChannelTest.test_channel_name,
                            channel_context_name="reader",
                            status=ChannelStatus.Opened
                        )
                    )), routing_key=RMQChannelTest.test_reader_status_queue_name)

                admin_channel.basic_publish(Message(
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
# Reader
# Test create resources
# Test create resources in group mode
# Test delete resources
# Test delete resources in group mode
# Test delete resources if not empty
# Test delete resources if used
# Test open channel w/o resources
# Test open channel w/ resources
# Test open channel if replicated
# Test has_records w/ records
# Test has_records w/o records
# Test retrieve w/ records
# Test retrieve w/o records
# Test retrieve w/o commit
# Test retrieve w/ commit
# Test can_close w/ records
# Test can_close w/o records
# Test can_close w/ connected channel (all combinations of healthy, stopped, closed)
# Test can_close as principal w/ connected channel
# Test max poll records
