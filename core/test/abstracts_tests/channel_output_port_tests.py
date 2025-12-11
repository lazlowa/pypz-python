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

from pypz.abstracts.channel_ports import (
    ChannelOutputPort,
    ParamKeyChannelConfig,
    ParamKeyChannelLocationConfig,
    ParamKeyPortOpenTimeoutMs,
)

from core.test.channels_tests.resources import (
    INPUT_STATE_POSTFIX,
    OUTPUT_STATE_POSTFIX,
    TEST_CONTROL_TRANSFER_MEDIUM,
    TEST_DATA_OFFSET_STORE,
    TEST_DATA_TRANSFER_MEDIUM,
)

from .channel_ports_resources import (
    TestChannelOutputPort,
    TestPipeline,
    TestPipelineWithReplicatedOperators,
)


class ChannelOutputPortTest(unittest.TestCase):

    def setUp(self) -> None:
        TEST_DATA_TRANSFER_MEDIUM.clear()
        TEST_CONTROL_TRANSFER_MEDIUM.clear()
        TEST_DATA_OFFSET_STORE.clear()

    def test_channel_output_port_without_channel_type_expect_error(self):
        channel_output_port = ChannelOutputPort("output")

        with self.assertRaises(AttributeError):
            channel_output_port._pre_execution()

    def test_channel_output_port_on_resource_creation_expect_initialized_channels_and_created_resources(
        self,
    ):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__create_resources": True}
        )

        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

        pipeline.writer.output_port._pre_execution()
        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )

        self.assertEqual(2, len(pipeline.writer.output_port._channel_writers))

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertTrue(channel_writer.is_resource_created())
            self.assertTrue(
                (
                    pipeline.reader.input_port_a.get_full_name()
                    == channel_writer.get_channel_name()
                )
                or (
                    pipeline.reader.input_port_b.get_full_name()
                    == channel_writer.get_channel_name()
                )
            )
            self.assertEqual("local", channel_writer.get_location())
            self.assertEqual(
                {"return__create_resources": True}, channel_writer.get_configuration()
            )

    def test_channel_output_port_on_resource_creation_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__create_resources": False}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertFalse(pipeline.writer.output_port._on_resource_creation())

        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

        self.assertEqual(2, len(pipeline.writer.output_port._channel_writers))

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertFalse(channel_writer.is_resource_created())
            self.assertTrue(
                (
                    pipeline.reader.input_port_a.get_full_name()
                    == channel_writer.get_channel_name()
                )
                or (
                    pipeline.reader.input_port_b.get_full_name()
                    == channel_writer.get_channel_name()
                )
            )
            self.assertEqual("local", channel_writer.get_location())
            self.assertEqual(
                {"return__create_resources": False}, channel_writer.get_configuration()
            )

    def test_channel_output_port_on_resource_creation_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"raise__create_resources": "Test Error"}
        )

        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

        pipeline.writer.output_port._pre_execution()

        with self.assertRaises(AttributeError):
            pipeline.writer.output_port._on_resource_creation()

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertFalse(channel_writer.is_resource_created())

    def test_channel_output_port_on_resource_creation_in_replica_expect_no_resource_created(
        self,
    ):
        pipeline = TestPipelineWithReplicatedOperators("pipeline")
        pipeline.reader.input_port_a.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.reader.input_port_a.set_parameter(
            ParamKeyChannelConfig, {"return__create_resources": True}
        )

        writer_replica = pipeline.writer.get_replica(0)

        writer_replica.output_port._pre_execution()

        self.assertTrue(writer_replica.output_port._on_resource_creation())

        for channel_writer in writer_replica.output_port._channel_writers:
            self.assertFalse(channel_writer.is_resource_created())

        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

    def test_channel_output_port_on_resource_deletion(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__delete_resources": True}
        )

        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )

        self.assertTrue(pipeline.writer.output_port._on_resource_deletion())

        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertTrue(channel_writer.is_resource_deleted())

    def test_channel_output_port_on_resource_deletion_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__delete_resources": False}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertFalse(pipeline.writer.output_port._on_resource_deletion())

        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertFalse(channel_writer.is_resource_deleted())

    def test_channel_output_port_on_resource_deletion_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"raise__delete_resources": "Test Error"}
        )

        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name(), TEST_DATA_TRANSFER_MEDIUM
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )

        with self.assertRaises(InterruptedError):
            pipeline.writer.output_port._on_resource_deletion()

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertFalse(channel_writer.is_resource_deleted())

    def test_channel_output_port_on_port_open_expect_opened_ports(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__open_channel": True}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            self.assertTrue(pipeline.writer.output_port._on_port_open())

            for channel_writer in pipeline.writer.output_port._channel_writers:
                self.assertTrue(channel_writer.is_channel_started())
                self.assertTrue(channel_writer.is_channel_open())

            self.assertIsNotNone(
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
            )
            self.assertIsNotNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
                ]
            )
            self.assertIsNotNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
                ]
            )
            self.assertIsNotNone(
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
            )
            self.assertIsNotNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
                ]
            )
            self.assertIsNotNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
                ]
            )
        finally:
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_output_port_on_port_open_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__open_channel": False}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            self.assertFalse(pipeline.writer.output_port._on_port_open())

            for channel_writer in pipeline.writer.output_port._channel_writers:
                self.assertFalse(channel_writer.is_channel_started())
                self.assertFalse(channel_writer.is_channel_open())

            self.assertIsNone(
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
            )
            self.assertIsNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
                ]
            )
            self.assertIsNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
                ]
            )
            self.assertIsNone(
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
            )
            self.assertIsNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
                ]
            )
            self.assertIsNone(
                TEST_CONTROL_TRANSFER_MEDIUM[
                    pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
                ]
            )
        finally:
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_output_port_on_port_open_unfinished_with_timeout(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(ParamKeyPortOpenTimeoutMs, 10)
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__open_channel": False}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertFalse(pipeline.writer.output_port._on_port_open())
        time.sleep(0.020)
        with self.assertRaises(TimeoutError):
            pipeline.writer.output_port._on_port_open()

    def test_channel_output_port_on_port_open_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"raise__open_channel": "Test Error"}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())

        try:
            with self.assertRaises(AttributeError):
                pipeline.writer.output_port._on_port_open()

            for channel_writer in pipeline.writer.output_port._channel_writers:
                self.assertFalse(channel_writer.is_channel_started())
                self.assertFalse(channel_writer.is_channel_open())
        finally:
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_output_port_on_port_close_expect_closed_ports(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__close_channel": True}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_port_open())
        self.assertTrue(pipeline.writer.output_port._on_port_close())

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertTrue(channel_writer.is_channel_stopped())
            self.assertFalse(channel_writer.is_channel_open())

        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )

    def test_channel_output_port_on_port_close_unfinished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"return__close_channel": False}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_port_open())
        self.assertFalse(pipeline.writer.output_port._on_port_close())

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertTrue(channel_writer.is_channel_stopped())
            self.assertTrue(channel_writer.is_channel_open())

        self.assertIsNotNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()]
        )
        self.assertIsNotNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNotNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_a.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNotNone(
            TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()]
        )
        self.assertIsNotNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNotNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                pipeline.reader.input_port_b.get_full_name() + INPUT_STATE_POSTFIX
            ]
        )

    def test_channel_output_port_on_port_close_error_raised(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelConfig, {"raise__close_channel": "Test Error"}
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_port_open())
        with self.assertRaises(InterruptedError):
            pipeline.writer.output_port._on_port_close()

        for channel_writer in pipeline.writer.output_port._channel_writers:
            self.assertTrue(channel_writer.is_channel_stopped())
            self.assertTrue(channel_writer.is_channel_open())

    def test_channel_output_port_send_data(self):
        pipeline = TestPipeline("pipeline")
        pipeline.writer.output_port.set_parameter(
            ParamKeyChannelLocationConfig, "local"
        )

        pipeline.writer.output_port._pre_execution()

        self.assertTrue(pipeline.writer.output_port._on_resource_creation())
        self.assertTrue(pipeline.writer.output_port._on_port_open())

        try:
            pipeline.writer.output_port.send(["0", "1", "2"])
            self.assertEqual(
                ["0", "1", "2"],
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()],
            )
            self.assertEqual(
                ["0", "1", "2"],
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()],
            )

            pipeline.writer.output_port.send(["3", "4", "5"])
            self.assertEqual(
                ["0", "1", "2", "3", "4", "5"],
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_a.get_full_name()],
            )
            self.assertEqual(
                ["0", "1", "2", "3", "4", "5"],
                TEST_DATA_TRANSFER_MEDIUM[pipeline.reader.input_port_b.get_full_name()],
            )
        finally:
            self.assertTrue(pipeline.writer.output_port._on_port_close())

    def test_channel_output_port_without_parent_context(self):
        output_port: ChannelOutputPort = TestChannelOutputPort("output_port")

        try:
            output_port._pre_execution()
            output_port._on_resource_creation()
            output_port._on_port_open()
        except:  # noqa: E722
            self.fail()
        finally:
            output_port._on_port_close()
            output_port._on_resource_deletion()
