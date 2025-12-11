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
import uuid
from threading import Thread
from typing import Callable, Optional

from pypz.abstracts.channel_ports import ChannelInputPort, ChannelOutputPort
from pypz.core.channels.status import ChannelStatusMessage
from pypz.core.commons.utils import SynchronizedReference
from pypz.core.specs.misc import BlankInputPortPlugin, BlankOutputPortPlugin
from pypz.core.specs.pipeline import Pipeline


class ChannelSniffer:
    def __init__(self, input_port: ChannelInputPort, output_port: ChannelOutputPort):
        self.channel_name = (
            input_port.get_full_name()
            if input_port.get_group_principal() is None
            else input_port.get_group_principal().get_full_name()
        )

        context_uuid = uuid.uuid4()

        self.reader_sniffer = output_port.channel_writer_type(
            self.channel_name,
            BlankOutputPortPlugin(f"reader_{context_uuid}"),
            silent_mode=True,
        )
        self.reader_sniffer.set_location(input_port.get_parameter("channelLocation"))
        self.reader_sniffer.on_status_message_received(self.on_status_message)

        self.writer_sniffer = input_port.channel_reader_type(
            self.channel_name,
            BlankInputPortPlugin(f"writer_{context_uuid}"),
            silent_mode=True,
        )
        self.writer_sniffer.set_location(input_port.get_parameter("channelLocation"))
        self.writer_sniffer.on_status_message_received(self.on_status_message)

        self.on_status_update_callbacks: dict[
            str, Callable[[ChannelStatusMessage], None]
        ] = {}

    def open(self):
        return (
            self.reader_sniffer.is_channel_open()
            or self.reader_sniffer.invoke_open_channel()
        ) and (
            self.writer_sniffer.is_channel_open()
            or self.writer_sniffer.invoke_open_channel()
        )

    def close(self):
        return (
            not self.reader_sniffer.is_channel_open()
            or self.reader_sniffer.invoke_close_channel()
        ) and (
            not self.writer_sniffer.is_channel_open()
            or self.writer_sniffer.invoke_close_channel()
        )

    def sniff(self):
        self.writer_sniffer.invoke_sync_status_update()
        self.reader_sniffer.invoke_sync_status_update()

    def subscribe(
        self, channel_context_name, callback: Callable[[ChannelStatusMessage], None]
    ):
        self.on_status_update_callbacks[channel_context_name] = callback

    def on_status_message(self, status_messages: list[ChannelStatusMessage]):
        for status_message in status_messages:
            if status_message.channel_context_name in self.on_status_update_callbacks:
                self.on_status_update_callbacks[status_message.channel_context_name](
                    status_message
                )


class PipelineSniffer:
    def __init__(self, pipeline: Pipeline):
        self.channel_sniffers: dict[str, ChannelSniffer] = {}

        self.control_thread: Optional[Thread] = None

        self.all_opened: SynchronizedReference[bool] = SynchronizedReference(False)
        self.all_closed: SynchronizedReference[bool] = SynchronizedReference(False)

        for operator in pipeline.get_protected().get_nested_instances().values():
            if operator.is_principal():
                for input_port in (
                    operator.get_protected().get_nested_instances().values()
                ):
                    if isinstance(input_port, ChannelInputPort):
                        for output_port in input_port.get_connected_ports():
                            if isinstance(output_port, ChannelOutputPort):
                                channel_id = PipelineSniffer.get_channel_id(
                                    input_port, output_port
                                )
                                if channel_id in self.channel_sniffers:
                                    raise AttributeError(
                                        f"Channel id already existing: {channel_id}"
                                    )

                                self.channel_sniffers[channel_id] = ChannelSniffer(
                                    input_port, output_port
                                )

    @staticmethod
    def get_channel_id(
        input_port: ChannelInputPort, output_port: ChannelOutputPort
    ) -> str:
        principal_input_port = (
            input_port
            if input_port.get_group_principal() is None
            else input_port.get_group_principal()
        )
        principal_output_port = (
            output_port
            if output_port.get_group_principal() is None
            else output_port.get_group_principal()
        )

        return f"{principal_input_port.get_full_name()}+{principal_output_port.get_full_name()}"

    def get_channel_sniffer_by_port(
        self, input_port: ChannelInputPort, output_port: ChannelOutputPort
    ):
        return self.channel_sniffers[
            PipelineSniffer.get_channel_id(input_port, output_port)
        ]

    def _start(self):
        all_opened = True
        for channel_sniffer in self.channel_sniffers.values():
            if not channel_sniffer.open():
                all_opened = False
        self.all_opened.set(all_opened)

    def _stop(self):
        all_closed = True
        for channel_sniffer in self.channel_sniffers.values():
            if not channel_sniffer.close():
                all_closed = False
        self.all_closed.set(all_closed)

    def start(self) -> bool:
        if (self.control_thread is not None) and (not self.control_thread.is_alive()):
            self.control_thread = None

        if (self.control_thread is None) and (not self.all_opened.get()):
            self.control_thread = Thread(target=self._start)
            self.control_thread.start()

        return self.all_opened.get()

    def stop(self) -> bool:
        if (self.control_thread is not None) and (not self.control_thread.is_alive()):
            self.control_thread = None

        if (self.control_thread is None) and (not self.all_closed.get()):
            self.control_thread = Thread(target=self._stop)
            self.control_thread.start()

        return self.all_closed.get()
