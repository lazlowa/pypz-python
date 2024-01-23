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

from pypz.abstracts.channel_ports import ParamKeyChannelLocationConfig, ParamKeyChannelConfig, \
    ParamKeyPortOpenTimeoutMs, ChannelInputPort
from .channel_ports_resources import TestPipeline, TestChannelInputPort, \
    TestPipelineWithReplicatedOperators
from core.test.channels_tests.resources import TEST_DATA_TRANSFER_MEDIUM, TEST_CONTROL_TRANSFER_MEDIUM, \
    TEST_DATA_OFFSET_STORE


class ChannelInputPortTest(unittest.TestCase):

    def setUp(self) -> None:
        TEST_DATA_TRANSFER_MEDIUM.clear()
        TEST_CONTROL_TRANSFER_MEDIUM.clear()
        TEST_DATA_OFFSET_STORE.clear()

    def test_channel_input_port_without_channel_type_expect_error(self):
        channel_input_port = ChannelInputPort("input")

        with self.assertRaises(AttributeError):
            channel_input_port._pre_execution()

    def test_channel_input_port_on_resource_creation_expect_initialized_channels_and_created_resources(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__create_resources": True
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.reader.input_port_b._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.reader.input_port_b._on_resource_creation())

        self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_resource_created())
        self.assertIsNotNone(pipeline.reader.input_port_a._channel_reader)
        self.assertIsNotNone(pipeline.reader.input_port_b._channel_reader)

        self.assertEqual(pipeline.reader.input_port_a.get_full_name(),
                         pipeline.reader.input_port_a._channel_reader.get_channel_name())
        self.assertEqual(pipeline.reader.input_port_b.get_full_name(),
                         pipeline.reader.input_port_b._channel_reader.get_channel_name())
        self.assertEqual("local", pipeline.reader.input_port_a._channel_reader.get_location())
        self.assertEqual({"return__create_resources": True},
                         pipeline.reader.input_port_a._channel_reader.get_configuration())
        self.assertIsNone(pipeline.reader.input_port_b._channel_reader.get_location())
        self.assertEqual({}, pipeline.reader.input_port_b._channel_reader.get_configuration())

    def test_channel_input_port_on_resource_creation_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__create_resources": False
        })

        pipeline.reader.input_port_a._pre_execution()

        self.assertFalse(pipeline.reader.input_port_a._on_resource_creation())

        self.assertIsNotNone(pipeline.reader.input_port_a._channel_reader)

        self.assertEqual(pipeline.reader.input_port_a.get_full_name(),
                         pipeline.reader.input_port_a._channel_reader.get_channel_name())

        self.assertEqual("local", pipeline.reader.input_port_a._channel_reader.get_location())
        self.assertEqual({"return__create_resources": False},
                         pipeline.reader.input_port_a._channel_reader.get_configuration())

        self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_resource_created())

    def test_channel_input_port_on_resource_creation_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "raise__create_resources": "Test Error"
        })

        pipeline.reader.input_port_a._pre_execution()

        with self.assertRaises(AttributeError):
            pipeline.reader.input_port_a._on_resource_creation()

        self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_resource_created())

    def test_channel_input_port_on_resource_creation_in_replica_expect_no_resource_created(self):
        pipeline = TestPipelineWithReplicatedOperators("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__create_resources": True
        })

        reader_replica = pipeline.reader.get_replica(0)

        reader_replica.input_port_a._pre_execution()
        reader_replica.input_port_b._pre_execution()

        self.assertTrue(reader_replica.input_port_a._on_resource_creation())
        self.assertTrue(reader_replica.input_port_b._on_resource_creation())

        self.assertFalse(reader_replica.input_port_a._channel_reader.is_resource_created())
        self.assertFalse(reader_replica.input_port_b._channel_reader.is_resource_created())
        self.assertIsNotNone(reader_replica.input_port_a._channel_reader)
        self.assertIsNotNone(reader_replica.input_port_b._channel_reader)

    def test_channel_input_port_on_resource_deletion(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__delete_resources": True
        })

        pipeline.reader.input_port_a._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.reader.input_port_a._on_resource_deletion())
        self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_resource_deleted())

    def test_channel_input_port_on_resource_deletion_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__delete_resources": False
        })

        pipeline.reader.input_port_a._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertFalse(pipeline.reader.input_port_a._on_resource_deletion())
        self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_resource_deleted())

    def test_channel_input_port_on_resource_deletion_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "raise__delete_resources": "Test Error"
        })

        pipeline.reader.input_port_a._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())

        with self.assertRaises(AttributeError):
            pipeline.reader.input_port_a._on_resource_deletion()

        self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_resource_deleted())

    def test_channel_input_port_on_port_open_expect_opened_port(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__open_channel": True
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())
            self.assertTrue(pipeline.reader.input_port_a._on_port_open())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_open())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_started())
        finally:
            self.assertTrue(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_input_port_on_port_open_without_opened_output_expect_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__open_channel": True
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertFalse(pipeline.reader.input_port_a._on_port_open())
        self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_started())

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        self.assertFalse(pipeline.reader.input_port_a._on_port_open())
        self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_open())
        self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_started())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())
            self.assertTrue(pipeline.reader.input_port_a._on_port_open())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_open())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_started())
        finally:
            self.assertTrue(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_input_port_on_port_open_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__open_channel": False
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())
            self.assertFalse(pipeline.reader.input_port_a._on_port_open())
            self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_open())
            self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_started())
        finally:
            self.assertTrue(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_input_port_on_port_open_unfinished_with_timeout(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyPortOpenTimeoutMs, 10)
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__open_channel": False
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        self.assertTrue(pipeline.writer.output_port._on_port_open())
        self.assertFalse(pipeline.reader.input_port_a._on_port_open())
        time.sleep(0.020)
        try:
            with self.assertRaises(TimeoutError):
                pipeline.reader.input_port_a._on_port_open()
        finally:
            self.assertTrue(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_input_port_on_port_open_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "raise__open_channel": "Test Error"
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())
            with self.assertRaises(AttributeError):
                pipeline.reader.input_port_a._on_port_open()
        finally:
            self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_open())
            self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_started())
            self.assertTrue(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_input_port_on_port_close_expect_closed_ports(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__close_channel": True
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())
            self.assertTrue(pipeline.reader.input_port_a._on_port_open())
        finally:
            self.assertTrue(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_stopped())
            self.assertFalse(pipeline.reader.input_port_a._channel_reader.is_channel_open())

    def test_channel_input_port_on_port_close_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "return__close_channel": False
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())
            self.assertTrue(pipeline.reader.input_port_a._on_port_open())
        finally:
            self.assertFalse(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_stopped())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_open())

    def test_channel_input_port_on_port_close_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelLocationConfig, "local")
        pipeline.reader.input_port_a.set_parameter(ParamKeyChannelConfig, {
            "raise__close_channel": "Test Error"
        })

        pipeline.reader.input_port_a._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())
            self.assertTrue(pipeline.reader.input_port_a._on_port_open())
            with self.assertRaises(AttributeError):
                pipeline.reader.input_port_a._on_port_close()
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_stopped())
            self.assertTrue(pipeline.reader.input_port_a._channel_reader.is_channel_open())
        finally:
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_input_port_retrieve_data(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(ParamKeyChannelLocationConfig, "local")

        pipeline.reader.input_port_a._pre_execution()
        pipeline.reader.input_port_b._pre_execution()
        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertTrue(pipeline.reader.input_port_a._on_resource_creation())
        self.assertTrue(pipeline.reader.input_port_b._on_resource_creation())

        self.assertTrue(pipeline.writer.output_port._on_port_open())
        self.assertTrue(pipeline.reader.input_port_a._on_port_open())
        self.assertTrue(pipeline.reader.input_port_b._on_port_open())

        try:
            pipeline.writer.output_port.send(["0", "1", "2"])
            self.assertEqual(["0", "1", "2"], pipeline.reader.input_port_a.retrieve())
            self.assertEqual([], pipeline.reader.input_port_a.retrieve())
            self.assertEqual(["0", "1", "2"], pipeline.reader.input_port_b.retrieve())
            self.assertEqual([], pipeline.reader.input_port_b.retrieve())

            pipeline.writer.output_port.send(["3", "4", "5"])
            self.assertEqual(["3", "4", "5"], pipeline.reader.input_port_a.retrieve())
            self.assertEqual(["3", "4", "5"], pipeline.reader.input_port_b.retrieve())

        finally:
            self.assertTrue(pipeline.reader.input_port_b._on_port_close())
            self.assertTrue(pipeline.reader.input_port_a._on_port_close())
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_input_port_without_parent_context(self):
        input_port: ChannelInputPort = TestChannelInputPort("input_port")

        try:
            input_port._pre_execution()
            input_port._on_resource_creation()
            input_port._on_port_open()
        except:
            self.fail()
        finally:
            input_port._on_port_close()
            input_port._on_resource_deletion()
