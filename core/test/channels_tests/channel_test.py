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

from pypz.core.channels.status import ChannelStatus, ChannelStatusMessage
from pypz.core.specs.misc import (
    BlankInputPortPlugin,
    BlankOutputPortPlugin,
    BlankPortPlugin,
)

from core.test.channels_tests.resources import (
    INPUT_STATE_POSTFIX,
    OUTPUT_STATE_POSTFIX,
    TEST_CONTROL_TRANSFER_MEDIUM,
    TEST_DATA_OFFSET_STORE,
    TEST_DATA_TRANSFER_MEDIUM,
    TestChannel,
    TestChannelReader,
    TestChannelWriter,
    TestGroupedOperator,
)


class ChannelTest(unittest.TestCase):

    def setUp(self) -> None:
        TEST_DATA_TRANSFER_MEDIUM.clear()
        TEST_CONTROL_TRANSFER_MEDIUM.clear()
        TEST_DATA_OFFSET_STORE.clear()

    def test_channel_with_invalid_return_types_expect_error(self):
        channel = TestChannel(channel_name="channel", context=BlankPortPlugin("owner"))

        with self.assertRaises(TypeError):
            channel.invoke_resource_creation()

        with self.assertRaises(TypeError):
            channel.invoke_open_channel()

        channel._channel_opened = True

        with self.assertRaises(TypeError):
            channel.invoke_close_channel()

        with self.assertRaises(TypeError):
            channel.invoke_resource_deletion()

    def test_create_io_channel_pair_and_send_data_expect_success(self):
        input_channel = TestChannelReader(
            channel_name="test_channel", context=BlankInputPortPlugin("input_owner")
        )
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )

        self.assertNotIn(input_channel.get_channel_name(), TEST_DATA_TRANSFER_MEDIUM)
        self.assertNotIn(
            input_channel.get_channel_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertNotIn(
            input_channel.get_channel_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )

        output_channel.invoke_resource_creation()

        self.assertIn(input_channel.get_channel_name(), TEST_DATA_TRANSFER_MEDIUM)
        self.assertIn(
            input_channel.get_channel_name() + OUTPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertIn(
            input_channel.get_channel_name() + INPUT_STATE_POSTFIX,
            TEST_CONTROL_TRANSFER_MEDIUM,
        )
        self.assertIsNone(TEST_DATA_TRANSFER_MEDIUM[input_channel.get_channel_name()])
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                input_channel.get_channel_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                input_channel.get_channel_name() + INPUT_STATE_POSTFIX
            ]
        )
        self.assertTrue(output_channel.is_resource_created())

        output_channel.invoke_open_channel()
        self.assertTrue(output_channel.is_channel_open())
        self.assertIsNotNone(
            TEST_DATA_TRANSFER_MEDIUM[input_channel.get_channel_name()]
        )
        self.assertIsNotNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                input_channel.get_channel_name() + OUTPUT_STATE_POSTFIX
            ]
        )
        self.assertIsNotNone(
            TEST_CONTROL_TRANSFER_MEDIUM[
                input_channel.get_channel_name() + INPUT_STATE_POSTFIX
            ]
        )

        input_channel.invoke_resource_creation()
        self.assertTrue(input_channel.is_resource_created())
        input_channel.invoke_open_channel()
        input_channel.set_initial_record_offset_auto()

        try:
            self.assertTrue(output_channel.is_channel_open())

            output_channel.invoke_write_records(["0", "1", "2"])
            self.assertEqual(["0", "1", "2"], input_channel.invoke_read_records())
            self.assertEqual(3, output_channel.get_written_record_count())
            self.assertEqual(3, input_channel.get_read_record_count())
            self.assertEqual(3, input_channel.get_read_record_offset())

            output_channel.invoke_write_records(["3", "4", "5"])
            self.assertEqual(["3", "4", "5"], input_channel.invoke_read_records())
            self.assertEqual(6, output_channel.get_written_record_count())
            self.assertEqual(6, input_channel.get_read_record_count())
            self.assertEqual(6, input_channel.get_read_record_offset())
        finally:
            input_channel.invoke_close_channel()
            input_channel.invoke_resource_deletion()

            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

        self.assertTrue(input_channel.is_resource_deleted())
        self.assertFalse(output_channel.is_channel_open())
        self.assertTrue(output_channel.is_resource_deleted())

    def test_open_input_channel_with_closed_output_channel_expect_return_false(self):
        input_channel = TestChannelReader(
            channel_name="test_channel", context=BlankInputPortPlugin("input_owner")
        )
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )

        self.assertFalse(input_channel.invoke_open_channel())
        output_channel.invoke_resource_creation()
        self.assertFalse(input_channel.invoke_open_channel())

    def test_grouped_input_channels_and_connection_statistics(self):
        operator = TestGroupedOperator("operator")
        input_channel = TestChannelReader(
            channel_name="test_channel", context=operator.input_port
        )
        input_channel_1 = TestChannelReader(
            channel_name="test_channel", context=operator.input_port_1
        )
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )

        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        input_channel.invoke_resource_creation()
        input_channel.invoke_open_channel()
        input_channel.set_initial_record_offset_auto()

        input_channel_1.invoke_resource_creation()
        input_channel_1.invoke_open_channel()
        input_channel_1.set_initial_record_offset_auto()

        try:
            output_channel.invoke_write_records(["0", "1", "2"])
            self.assertEqual(["0", "1", "2"], input_channel.invoke_read_records())
            self.assertEqual(["0", "1", "2"], input_channel_1.invoke_read_records())

            output_channel.invoke_write_records(["3", "4", "5"])
            self.assertEqual(["3", "4", "5"], input_channel.invoke_read_records())
            self.assertEqual(["3", "4", "5"], input_channel_1.invoke_read_records())

        finally:
            input_channel_1.invoke_close_channel()
            input_channel_1.invoke_resource_deletion()

            input_channel.invoke_close_channel()
            input_channel.invoke_resource_deletion()

            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

        self.assertEqual(2, output_channel.retrieve_all_connected_channel_count())
        self.assertEqual(
            {"test_channel@operator.input_port", "test_channel@operator.input_port_1"},
            output_channel.retrieve_connected_channel_unique_names(),
        )

    def test_create_io_channel_pair_and_invoke_offset_commit_and_load_expect_success(
        self,
    ):
        input_channel = TestChannelReader(
            channel_name="test_channel", context=BlankInputPortPlugin("input_owner")
        )
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )

        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        input_channel.invoke_resource_creation()
        input_channel.invoke_open_channel()
        input_channel.set_initial_record_offset_auto()

        try:
            output_channel.invoke_write_records(["0", "1", "2"])
            self.assertEqual(["0", "1", "2"], input_channel.invoke_read_records())
            input_channel.set_initial_record_offset_auto()
            self.assertEqual(0, input_channel.get_read_record_offset())
            self.assertEqual(["0", "1", "2"], input_channel.invoke_read_records())
            input_channel.invoke_commit_current_read_offset()
            input_channel.set_initial_record_offset_auto()
            self.assertEqual(3, input_channel.get_read_record_offset())

            output_channel.invoke_write_records(["3", "4", "5"])
            self.assertEqual(["3", "4", "5"], input_channel.invoke_read_records())
            input_channel.set_initial_record_offset_auto()
            self.assertEqual(3, input_channel.get_read_record_offset())
            self.assertEqual(["3", "4", "5"], input_channel.invoke_read_records())
        finally:
            input_channel.invoke_close_channel()
            input_channel.invoke_resource_deletion()

            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_retrieve_healthy_connected_channel_count(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertEqual(
                {
                    "test_channel@input_owner_0",
                    "test_channel@input_owner_1",
                    "test_channel@input_owner_2",
                },
                output_channel.retrieve_connected_channel_unique_names(),
            )
            self.assertEqual(
                3, output_channel.retrieve_healthy_connected_channel_count()
            )
        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_is_any_connected_channel_healthy_and_not_stopped(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(
                output_channel.is_any_connected_channel_healthy_and_not_stopped()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Stopped,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Stopped,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Stopped,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(
                output_channel.is_any_connected_channel_healthy_and_not_stopped()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Stopped,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Stopped,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(
                output_channel.is_any_connected_channel_healthy_and_not_stopped()
            )
        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_is_any_connected_channel_healthy_and_not_closed(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(
                output_channel.is_any_connected_channel_healthy_and_not_closed()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Closed,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(
                output_channel.is_any_connected_channel_healthy_and_not_closed()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Closed,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(
                output_channel.is_any_connected_channel_healthy_and_not_closed()
            )
        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_is_any_connected_channel_healthy_and_not_stopped_and_not_closed(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(
                output_channel.is_any_connected_channel_healthy_and_not_stopped_and_not_closed()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Closed,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(
                output_channel.is_any_connected_channel_healthy_and_not_stopped_and_not_closed()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Stopped,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Stopped,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Stopped,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(
                output_channel.is_any_connected_channel_healthy_and_not_stopped_and_not_closed()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Stopped,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(
                output_channel.is_any_connected_channel_healthy_and_not_stopped_and_not_closed()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Stopped,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Closed,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(
                output_channel.is_any_connected_channel_healthy_and_not_stopped_and_not_closed()
            )
        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_is_any_connected_channels_unhealthy(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                        timestamp=1,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(output_channel.is_any_connected_channels_unhealthy())

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(output_channel.is_any_connected_channels_unhealthy())

        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_is_any_connected_channels_healthy(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                        timestamp=1,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                        timestamp=1,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Opened,
                        timestamp=1,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(output_channel.is_any_connected_channels_healthy())

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(output_channel.is_any_connected_channels_healthy())

        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_retrieve_channel_names_in_different_states(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                        timestamp=1,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Started,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_3",
                        status=ChannelStatus.Stopped,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_4",
                        status=ChannelStatus.Closed,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_5",
                        status=ChannelStatus.Error,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertEqual(
                {"test_channel@input_owner_0"},
                output_channel.retrieve_connected_channel_unique_names(
                    lambda checker: not checker.is_channel_healthy()
                ),
            )
            self.assertEqual(
                {"test_channel@input_owner_0", "test_channel@input_owner_1"},
                output_channel.retrieve_connected_channel_unique_names(
                    lambda checker: checker.is_channel_opened()
                ),
            )
            self.assertEqual(
                {"test_channel@input_owner_2"},
                output_channel.retrieve_connected_channel_unique_names(
                    lambda checker: checker.is_channel_started()
                ),
            )
            self.assertEqual(
                {"test_channel@input_owner_3"},
                output_channel.retrieve_connected_channel_unique_names(
                    lambda checker: checker.is_channel_stopped()
                ),
            )
            self.assertEqual(
                {"test_channel@input_owner_4"},
                output_channel.retrieve_connected_channel_unique_names(
                    lambda checker: checker.is_channel_closed()
                ),
            )
            self.assertEqual(
                {"test_channel@input_owner_5"},
                output_channel.retrieve_connected_channel_unique_names(
                    lambda checker: checker.is_channel_error()
                ),
            )

        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()

    def test_is_all_connected_input_channels_acknowledged(self):
        output_channel = TestChannelWriter(
            channel_name="test_channel", context=BlankOutputPortPlugin("output_owner")
        )
        output_channel.invoke_resource_creation()
        output_channel.invoke_open_channel()

        try:
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Opened,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Opened,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(
                output_channel.is_all_connected_input_channels_acknowledged()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_0",
                        status=ChannelStatus.Acknowledged,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertFalse(
                output_channel.is_all_connected_input_channels_acknowledged()
            )

            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_1",
                        status=ChannelStatus.Acknowledged,
                    )
                )
            )
            TEST_CONTROL_TRANSFER_MEDIUM["test_channel.input.state"].append(
                str(
                    ChannelStatusMessage(
                        channel_name="test_channel",
                        channel_context_name="input_owner_2",
                        status=ChannelStatus.Acknowledged,
                    )
                )
            )

            output_channel.invoke_sync_status_update()
            self.assertTrue(
                output_channel.is_all_connected_input_channels_acknowledged()
            )
        finally:
            output_channel.invoke_close_channel()
            output_channel.invoke_resource_deletion()
