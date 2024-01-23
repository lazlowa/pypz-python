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

from pypz.core.channels.status import ChannelStatusMonitor, ChannelStatusMessage, ChannelStatus
from pypz.core.commons.utils import current_time_millis
from core.test.channels_tests.resources import TestChannel
from pypz.core.specs.misc import BlankPortPlugin


class ChannelStatusMonitorTest(unittest.TestCase):

    def test_status_update_expect_success(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.last_update_sent_timestamp_ms = 0

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Opened,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))
        self.assertTrue(monitor.is_channel_opened())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Started,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))
        self.assertTrue(monitor.is_channel_started())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Stopped,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))
        self.assertTrue(monitor.is_channel_stopped())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Closed,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))
        self.assertTrue(monitor.is_channel_closed())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Error,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))
        self.assertTrue(monitor.is_channel_error())

    def test_status_update_with_invalid_time_expect_no_update(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.last_update_sent_timestamp_ms = 10

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Opened,
                                            payload=["payload1", "payload2"],
                                            timestamp=11))
        self.assertTrue(monitor.is_channel_opened())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Started,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))
        self.assertTrue(monitor.is_channel_opened())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Stopped,
                                            payload=["payload1", "payload2"],
                                            timestamp=3))
        self.assertTrue(monitor.is_channel_opened())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Closed,
                                            payload=["payload1", "payload2"],
                                            timestamp=4))
        self.assertTrue(monitor.is_channel_opened())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Error,
                                            payload=["payload1", "payload2"],
                                            timestamp=5))
        self.assertTrue(monitor.is_channel_opened())

    def test_status_update_from_error_state_expect_no_update(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.last_update_sent_timestamp_ms = 0

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Error,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))
        self.assertTrue(monitor.is_channel_error())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Stopped,
                                            payload=["payload1", "payload2"],
                                            timestamp=2))
        self.assertTrue(monitor.is_channel_error())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Closed,
                                            payload=["payload1", "payload2"],
                                            timestamp=3))
        self.assertTrue(monitor.is_channel_error())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Started,
                                            payload=["payload1", "payload2"],
                                            timestamp=4))
        self.assertTrue(monitor.is_channel_started())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Error,
                                            payload=["payload1", "payload2"],
                                            timestamp=5))
        self.assertTrue(monitor.is_channel_error())

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Opened,
                                            payload=["payload1", "payload2"],
                                            timestamp=6))
        self.assertTrue(monitor.is_channel_opened())

    def on_status_update_with_payload_func(self, status_message: ChannelStatusMessage):
        self.assertEqual("channelName", status_message.channel_name)
        self.assertEqual("ownerName", status_message.channel_context_name)
        self.assertEqual(ChannelStatus.Started, status_message.status)
        self.assertEqual(1, status_message.timestamp)
        self.assertEqual(2, len(status_message.payload))
        self.assertEqual("payload1", status_message.payload[0])
        self.assertEqual("payload2", status_message.payload[1])

    def test_update_status_monitor_with_payload_and_callback_expect_success(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.last_update_sent_timestamp_ms = 0

        monitor.on_status_update(self.on_status_update_with_payload_func)

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Started,
                                            payload=["payload1", "payload2"],
                                            timestamp=1))

    def on_status_update_without_payload_func(self, status_message: ChannelStatusMessage):
        self.assertEqual(1, status_message.timestamp)
        self.assertIsNone(status_message.payload)

    def test_test_update_status_monitor_without_payload_and_callback_expect_success(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.last_update_sent_timestamp_ms = 0

        monitor.on_status_update(self.on_status_update_without_payload_func)

        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Started,
                                            payload=None,
                                            timestamp=1))

    def test_update_status_monitor_with_invalid_channel_name_expect_error(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.last_update_sent_timestamp_ms = 0

        with self.assertRaises(AttributeError):
            monitor.update(ChannelStatusMessage(channel_name="invalidChannelName",
                                                channel_context_name="ownerName",
                                                status=ChannelStatus.Started,
                                                payload=["payload1", "payload2"],
                                                timestamp=1))

        with self.assertRaises(AttributeError):
            monitor.update(ChannelStatusMessage(channel_name="channelName",
                                                channel_context_name="invalidOwnerName",
                                                status=ChannelStatus.Started,
                                                payload=["payload1", "payload2"],
                                                timestamp=1))

    def test_health_check_no_status_update_expect_healthy(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        self.assertTrue(monitor.is_channel_healthy())

    def test_health_check_no_status_update_expect_unhealthy(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.monitor_start_timestamp = current_time_millis() - ChannelStatusMonitor.ValidPatienceTimeInMs - 100
        self.assertFalse(monitor.is_channel_healthy())

    def test_health_check_with_status_update_expect_healthy(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Started,
                                            payload=None,
                                            timestamp=current_time_millis()))
        self.assertTrue(monitor.is_channel_healthy())

    def test_health_check_with_status_update_expect_unhealthy(self):
        monitor = ChannelStatusMonitor("channelName", "ownerName")
        monitor.update(ChannelStatusMessage(channel_name="channelName",
                                            channel_context_name="ownerName",
                                            status=ChannelStatus.Started,
                                            payload=None,
                                            timestamp=1))
        self.assertFalse(monitor.is_channel_healthy())

    def test_get_channel_names_if_channel1_name_contains_string_expect_success(self):
        channel_base = TestChannel("testChannel", BlankPortPlugin("ownerName"), None)

        channel_base._status_map["channel1"] = ChannelStatusMonitor("channel1", "ownerName", None, channel_base._logger)
        channel_base._status_map.get("channel1").update(ChannelStatusMessage(channel_name="channel1",
                                                                             channel_context_name="ownerName",
                                                                             status=ChannelStatus.Started,
                                                                             payload=None,
                                                                             timestamp=1))

        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: "channel" in flt.get_channel_unique_name())))
        self.assertEqual(0, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: "wrong" in flt.get_channel_unique_name())))
        self.assertEqual("channel1", channel_base.retrieve_connected_channel_unique_names(
            lambda flt: "channel" in flt.get_channel_unique_name()).pop())

    def test_get_channel_names_if_with_valid_statuses_expect_success(self):
        channel_base = TestChannel("testChannel", BlankPortPlugin("owner"), None)

        channel_base._status_map["channel1"] = ChannelStatusMonitor("channel1", "ownerName", None, channel_base._logger)
        channel_base._status_map.get("channel1").update(ChannelStatusMessage(channel_name="channel1",
                                                                             channel_context_name="ownerName",
                                                                             status=ChannelStatus.Opened,
                                                                             payload=None,
                                                                             timestamp=1))

        channel_base._status_map["channel2"] = ChannelStatusMonitor("channel2", "ownerName", None, channel_base._logger)
        channel_base._status_map.get("channel2").update(ChannelStatusMessage(channel_name="channel2",
                                                                             channel_context_name="ownerName",
                                                                             status=ChannelStatus.Closed,
                                                                             payload=None,
                                                                             timestamp=1))

        channel_base._status_map["channel3"] = ChannelStatusMonitor("channel3", "ownerName", None, channel_base._logger)
        channel_base._status_map.get("channel3").update(ChannelStatusMessage(channel_name="channel3",
                                                                             channel_context_name="ownerName",
                                                                             status=ChannelStatus.Error,
                                                                             payload=None,
                                                                             timestamp=1))

        channel_base._status_map["channel4"] = ChannelStatusMonitor("channel4", "ownerName", None, channel_base._logger)
        channel_base._status_map.get("channel4").update(ChannelStatusMessage(channel_name="channel4",
                                                                             channel_context_name="ownerName",
                                                                             status=ChannelStatus.Stopped,
                                                                             payload=None,
                                                                             timestamp=1))

        channel_base._status_map["channel5"] = ChannelStatusMonitor("channel5", "ownerName", None, channel_base._logger)
        channel_base._status_map.get("channel5").update(ChannelStatusMessage(channel_name="channel5",
                                                                             channel_context_name="ownerName",
                                                                             status=ChannelStatus.Started,
                                                                             payload=None,
                                                                             timestamp=1))

        channel_base._status_map["channel6"] = ChannelStatusMonitor("channel6", "ownerName", None, channel_base._logger)

        self.assertEqual(6, len(channel_base.retrieve_connected_channel_unique_names(None)))

        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_opened())))
        self.assertEqual("channel1", list(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_opened()))[0])

        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_closed())))
        self.assertEqual("channel2", list(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_closed()))[0])

        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_error())))
        self.assertEqual("channel3", list(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_error()))[0])

        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_stopped())))
        self.assertEqual("channel4", list(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_stopped()))[0])

        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_started())))
        self.assertEqual("channel5", list(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_started()))[0])

        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_not_initialised())))
        self.assertEqual("channel6", list(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_not_initialised()))[0])

        """
         * By default, the update timestamp is the current system timestamp on the creation of the
         * monitor object. This will then be updated upon updating the status. Since we don't update
         * the 'channel6', the timestamp will remain the system timestamp, hence it will be healthy.
         * That is the reason, why we assert the size of the healthy channel names to 1.
        """
        self.assertEqual(1, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_healthy())))
        self.assertEqual(0, len(channel_base.retrieve_connected_channel_unique_names(
            lambda flt: flt.is_channel_healthy() and flt.is_channel_opened())))
