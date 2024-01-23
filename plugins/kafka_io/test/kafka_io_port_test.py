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

from kafka import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

from pypz.plugins.kafka_io.channels import ReaderStatusTopicNameExtension, WriterStatusTopicNameExtension
from plugins.kafka_io.test.resources import TestPipeline


class KafkaIOPortTest(unittest.TestCase):

    bootstrap_url = "localhost:9092"
    test_admin_client: KafkaAdminClient
    test_channel_name = "pipeline.reader.input_port"
    test_reader_status_name = test_channel_name + ReaderStatusTopicNameExtension
    test_writer_status_name = test_channel_name + WriterStatusTopicNameExtension

    @classmethod
    def setUpClass(cls) -> None:
        time.sleep(2)

        cls.test_admin_client: KafkaAdminClient = KafkaAdminClient(bootstrap_servers=KafkaIOPortTest.bootstrap_url)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            cls.test_admin_client.delete_topics([
                cls.test_channel_name,
                cls.test_writer_status_name,
                cls.test_reader_status_name
            ])
        except UnknownTopicOrPartitionError:
            pass

        cls.test_admin_client.close()

    def setUp(self) -> None:
        try:
            KafkaIOPortTest.test_admin_client.delete_topics([
                KafkaIOPortTest.test_channel_name,
                KafkaIOPortTest.test_writer_status_name,
                KafkaIOPortTest.test_reader_status_name
            ])
        except UnknownTopicOrPartitionError:
            pass

    def test_almost_everything(self):
        """
        This test method attempts to test as many things as it can. Although it is very
        well known that test cases shall be separated, setting up the context takes
        quite long time, hence we set up once and test different cases. The explanation
        of the cases can be found in the code.
        """

        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter("##channelLocation", KafkaIOPortTest.bootstrap_url)

        reader = pipeline.reader
        reader_0 = pipeline.reader.get_replica(0)
        writer = pipeline.writer
        writer_0 = pipeline.writer.get_replica(0)

        try:
            reader.input_port._pre_execution()
            reader_0.input_port._pre_execution()
            writer.output_port._pre_execution()
            writer_0.output_port._pre_execution()

            self.assertTrue(reader_0.input_port._on_resource_creation())
            self.assertTrue(writer.output_port._on_resource_creation())
            self.assertTrue(writer_0.output_port._on_resource_creation())

            # Case #1
            # Writer and replica reader channels cannot open, until
            # resources are not created. Resource us created by the
            # principal reader instance, hence expect False.
            self.assertFalse(writer.output_port._on_port_open())
            self.assertFalse(writer_0.output_port._on_port_open())
            self.assertFalse(reader_0.input_port._on_port_open())

            self.assertTrue(reader.input_port._on_resource_creation())

            time.sleep(1)

            # Case #2
            # Principal reader instance cannot open, until at least
            # one writer is not connected. If the parameter
            # '_sync_connections_open' is set to True, then the
            # principal reader shall wait for all writers.
            self.assertFalse(reader.input_port._on_port_open())
            self.assertFalse(reader_0.input_port._on_port_open())

            # Case #3
            # All resources created and all channels are opened,
            # hence principal reader can open as well.
            self.assertTrue(writer.output_port._on_port_open())

            time.sleep(1)

            self.assertTrue(reader_0.input_port._on_port_open())
            self.assertTrue(reader.input_port._on_port_open())
            self.assertTrue(writer_0.output_port._on_port_open())

            reader._on_init()
            reader_0._on_init()
            writer._on_init()
            writer_0._on_init()

            # Case #4
            # On invoking on_running, we expect writer and reader to
            # return True, since those are not yet finished.
            self.assertFalse(writer._on_running())
            self.assertFalse(writer_0._on_running())
            self.assertFalse(reader._on_running())
            self.assertFalse(reader_0._on_running())

            self.assertEqual([{"demoText": "record_0"}], reader.received_records)
            self.assertEqual([], reader_0.received_records)

            reader.input_port.commit_current_read_offset()
            reader_0.input_port.commit_current_read_offset()

            # Case #5
            # Since each writer writes 2 records, after the second
            # invocation expect True i.e., the writers finished.
            self.assertTrue(writer._on_running())
            self.assertTrue(writer_0._on_running())

            # Case #6
            # Since the writers are not yet closed, the method can_retrieve
            # of the readers will return True, hence the on_running will
            # return False.
            self.assertFalse(reader._on_running())
            self.assertFalse(reader_0._on_running())

            self.assertEqual([{"demoText": "record_0"}, {"demoText": "record_0"}], reader.received_records)
            self.assertEqual([{"demoText": "record_1"}], reader_0.received_records)

            reader.input_port.commit_current_read_offset()
            reader_0.input_port.commit_current_read_offset()

            # Case #7
            # Expect false, since outputs not yet closed
            # Notice that this does not apply to replica
            self.assertFalse(reader.input_port._on_port_close())

            # Expect True, since closing writers has no dependencies
            writer._on_shutdown()
            self.assertTrue(writer_0.output_port._on_port_close())
            self.assertTrue(writer.output_port._on_port_close())

            # Case #8
            # Outputs closed, so can_retrieve shall return False -> expect True
            self.assertTrue(reader._on_running())
            self.assertTrue(reader_0._on_running())
            self.assertEqual([{"demoText": "record_0"}, {"demoText": "record_0"}], reader.received_records)
            self.assertEqual([{"demoText": "record_1"}, {"demoText": "record_1"}], reader_0.received_records)
            reader._on_shutdown()

            # Case #9
            # Principal reader cannot close until replica reader is not closed
            self.assertFalse(reader.input_port._on_port_close())

            self.assertTrue(reader_0.input_port._on_port_close())

            # Waiting 1 sec to allow propagation through Kafka
            time.sleep(1)

            # Case #10
            # Expect true, since replica and readers are closed
            self.assertTrue(reader.input_port._on_port_close())

            self.assertTrue(writer_0.output_port._on_resource_deletion())
            self.assertTrue(writer.output_port._on_resource_deletion())

            # Case #11
            # Replica readers cannot delete resources
            self.assertTrue(reader_0.input_port._on_resource_deletion())

            existing_topics = KafkaIOPortTest.test_admin_client.list_topics()
            self.assertIn(KafkaIOPortTest.test_channel_name, existing_topics)
            self.assertIn(KafkaIOPortTest.test_writer_status_name, existing_topics)
            self.assertIn(KafkaIOPortTest.test_reader_status_name, existing_topics)

            # Case #12
            # In normal circumstances principal reader shall delete resources
            reader.input_port._error_occurred = False
            self.assertTrue(reader.input_port._on_resource_deletion())

            existing_topics = KafkaIOPortTest.test_admin_client.list_topics()
            self.assertNotIn(KafkaIOPortTest.test_channel_name, existing_topics)
            self.assertNotIn(KafkaIOPortTest.test_writer_status_name, existing_topics)
            self.assertNotIn(KafkaIOPortTest.test_reader_status_name, existing_topics)
        except Exception as e:
            writer.output_port._on_port_close()
            writer_0.output_port._on_port_close()
            reader_0.input_port._on_port_close()

            while not reader.input_port._on_port_close():
                pass

            reader.input_port._on_resource_deletion()
            reader_0.input_port._on_resource_deletion()
            self.fail(e)

    def test_reader_delete_resources_with_error_occurred_expect_no_resource_deletion(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter("##channelLocation", KafkaIOPortTest.bootstrap_url)
        reader = pipeline.reader

        try:
            reader.input_port._pre_execution()
            reader.input_port._on_resource_creation()
            reader.input_port._on_port_open()
            reader.input_port._on_error()
            reader.input_port._on_resource_deletion()

            existing_topics = KafkaIOPortTest.test_admin_client.list_topics()
            self.assertIn(KafkaIOPortTest.test_channel_name, existing_topics)
            self.assertIn(KafkaIOPortTest.test_writer_status_name, existing_topics)
            self.assertIn(KafkaIOPortTest.test_reader_status_name, existing_topics)
        except:  # noqa: E722
            self.fail()
        finally:
            reader.input_port._on_port_close()

    def test_reader_waits_until_all_writer_inited(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter("##channelLocation", KafkaIOPortTest.bootstrap_url)
        pipeline.reader.input_port._sync_connections_open = True

        reader = pipeline.reader
        reader_0 = pipeline.reader.get_replica(0)
        writer = pipeline.writer
        writer_0 = pipeline.writer.get_replica(0)

        try:
            reader.input_port._pre_execution()
            reader_0.input_port._pre_execution()
            writer.output_port._pre_execution()
            writer_0.output_port._pre_execution()

            self.assertTrue(reader.input_port._on_resource_creation())
            self.assertTrue(reader_0.input_port._on_resource_creation())
            self.assertTrue(writer.output_port._on_resource_creation())
            self.assertTrue(writer_0.output_port._on_resource_creation())

            self.assertFalse(reader.input_port._on_port_open())
            self.assertTrue(writer.output_port._on_port_open())
            self.assertFalse(reader.input_port._on_port_open())
            self.assertTrue(writer_0.output_port._on_port_open())
            self.assertTrue(reader.input_port._on_port_open())

            self.assertTrue(reader_0.input_port._on_port_open())

            self.assertTrue(writer_0.output_port._on_port_close())
            self.assertTrue(writer.output_port._on_port_close())
            self.assertTrue(reader_0.input_port._on_port_close())

            while not reader.input_port._on_port_close():
                pass

            self.assertTrue(writer_0.output_port._on_resource_deletion())
            self.assertTrue(writer.output_port._on_resource_deletion())
            self.assertTrue(reader_0.input_port._on_resource_deletion())

            # Waiting 1 sec to allow propagation through Kafka
            time.sleep(1)
            self.assertTrue(reader.input_port._on_resource_deletion())
        except Exception as e:
            writer.output_port._on_port_close()
            writer_0.output_port._on_port_close()
            reader_0.input_port._on_port_close()

            while not reader.input_port._on_port_close():
                pass

            reader.input_port._on_resource_deletion()
            reader_0.input_port._on_resource_deletion()
            self.fail(e)

    def test_writer_sends_record_to_reader_group_expect_all_reader_got_the_same_record(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter("##channelLocation", KafkaIOPortTest.bootstrap_url)

        reader = pipeline.reader
        reader_0 = pipeline.reader.get_replica(0)
        writer = pipeline.writer
        writer_0 = pipeline.writer.get_replica(0)

        try:
            reader.group_input_port._pre_execution()
            reader_0.group_input_port._pre_execution()
            writer.group_output_port._pre_execution()
            writer_0.group_output_port._pre_execution()

            self.assertTrue(reader.group_input_port._on_resource_creation())
            self.assertTrue(reader_0.group_input_port._on_resource_creation())
            self.assertTrue(writer.group_output_port._on_resource_creation())
            self.assertTrue(writer_0.group_output_port._on_resource_creation())

            self.assertTrue(writer.group_output_port._on_port_open())
            self.assertTrue(writer_0.group_output_port._on_port_open())
            self.assertTrue(reader.group_input_port._on_port_open())
            self.assertTrue(reader_0.group_input_port._on_port_open())

            writer.group_output_port.send([{"demoText": "from_writer_for_all"}])
            time.sleep(1)
            writer_0.group_output_port.send([{"demoText": "from_writer_0_for_all"}])
            time.sleep(1)

            self.assertEqual([{"demoText": "from_writer_for_all"}],
                             reader.group_input_port.retrieve())
            self.assertEqual([{"demoText": "from_writer_0_for_all"}],
                             reader.group_input_port.retrieve())
            self.assertEqual([{"demoText": "from_writer_for_all"}],
                             reader_0.group_input_port.retrieve())
            self.assertEqual([{"demoText": "from_writer_0_for_all"}],
                             reader_0.group_input_port.retrieve())

        finally:
            writer.group_output_port._on_port_close()
            writer_0.group_output_port._on_port_close()
            reader_0.group_input_port._on_port_close()

            while not reader.group_input_port._on_port_close():
                pass

            reader.group_input_port._on_resource_deletion()
            reader_0.group_input_port._on_resource_deletion()
