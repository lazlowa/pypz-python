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
import tkinter as tk

from pypz.abstracts.channel_ports import ChannelInputPort, ChannelOutputPort
from pypz.core.channels.status import ChannelStatusMessage, ChannelStatus
from pypz.core.specs.operator import Operator


class ViewConfig:
    window_width = 1400
    window_height = 600

    status_cell_count = 20
    status_cell_size = int(window_width / status_cell_count)

    operator_start_pos_x = 100
    operator_start_pos_y = 50
    operator_width = 300
    operator_height = 100
    operator_spacing_hor = 350
    operator_spacing_ver = 50
    replica_spacing_ver = 5

    channel_start_pos_x = 0
    channel_start_pos_y = 30
    channel_spacing_hor = 0
    channel_spacing_ver = 15
    channel_width = 120
    channel_cell_height = 12
    channel_height = channel_cell_height * 3

    grid_line_spacing_hor = 15
    grid_line_spacing_ver = 15


class ChannelView:
    def __init__(self, canvas: tk.Canvas, channel_reader_idx: int, channel_writer_idx: int):
        self.canvas = canvas

        self.channel_reader_idx = channel_reader_idx
        self.channel_writer_idx = channel_writer_idx
        self.channel_reader_views: list[ChannelRWView] = list()
        self.channel_writer_views: list[ChannelRWView] = list()

        self.x1 = None
        self.y1 = None
        self.x2 = None
        self.y2 = None

    def draw(self, next_available_y_position: int):
        first_channel_reader = min(self.channel_reader_views, key=lambda cr: cr.top)
        last_channel_reader = max(self.channel_reader_views, key=lambda cr: cr.top)
        first_channel_writer = min(self.channel_writer_views, key=lambda cr: cr.top)
        last_channel_writer = max(self.channel_writer_views, key=lambda cr: cr.top)

        channel_reader_x_min = first_channel_reader.left
        channel_reader_y_min = first_channel_reader.top + (first_channel_reader.bottom - first_channel_reader.top) / 2
        channel_reader_y_max = last_channel_reader.top + (last_channel_reader.bottom - last_channel_reader.top) / 2

        channel_writer_x_max = first_channel_writer.right
        channel_writer_y_min = first_channel_writer.top + (first_channel_writer.bottom - first_channel_writer.top) / 2
        channel_writer_y_max = last_channel_writer.top + (last_channel_writer.bottom - last_channel_writer.top) / 2

        self.x1 = channel_reader_x_min - 50 - 20 * self.channel_reader_idx
        self.y1 = channel_reader_y_min + (channel_reader_y_max - channel_reader_y_min) / 2
        self.x2 = channel_writer_x_max + 50 + 20 * self.channel_writer_idx
        self.y2 = channel_writer_y_min + (channel_writer_y_max - channel_writer_y_min) / 2

        for idx, channel_reader in enumerate(self.channel_reader_views):
            self.canvas.create_line(self.x1,
                                    channel_reader.top + (channel_reader.bottom - channel_reader.top) / 2,
                                    channel_reader_x_min - 5,
                                    channel_reader.top + (channel_reader.bottom - channel_reader.top) / 2,
                                    arrow=tk.LAST)

        for idx, channel_writer in enumerate(self.channel_writer_views):
            self.canvas.create_line(channel_writer.right + 5,
                                    channel_writer.top + (channel_writer.bottom - channel_writer.top) / 2,
                                    self.x2,
                                    channel_writer.top + (channel_writer.bottom - channel_writer.top) / 2)

        self.canvas.create_oval(self.x1 - 3, self.y1 - 3, self.x1 + 3, self.y1 + 3, fill="black")
        self.canvas.create_oval(self.x2 - 3, self.y2 - 3, self.x2 + 3, self.y2 + 3, fill="black")

        self.canvas.create_line(self.x1, self.y1, self.x1 - 10, self.y1)
        self.canvas.create_line(self.x2 + 10, self.y2, self.x2, self.y2)
        self.canvas.create_line(self.x2, channel_writer_y_min, self.x2, channel_writer_y_max)
        self.canvas.create_line(self.x1, channel_reader_y_min, self.x1, channel_reader_y_max)

        next_available_y_position += 20

        if self.x1 < self.x2:
            self.canvas.create_line(self.x1 - 10, self.y1, self.x1 - 10, next_available_y_position)
            self.canvas.create_line(self.x2 + 10, self.y2, self.x2 + 10, next_available_y_position)

            self.canvas.create_line(self.x1 - 10, next_available_y_position,
                                    self.x2 + 10, next_available_y_position)
        else:
            self.canvas.create_line(self.x1 - 10, self.y1, self.x2 + 10, self.y2)

        return next_available_y_position


class ChannelRWView:
    def __init__(self, canvas: tk.Canvas):
        self.canvas = canvas

        self.channel_box = None
        self.channel_text = None
        self.status_display_l = None
        self.status_display_r = None

        self.left = None
        self.top = None
        self.right = None
        self.bottom = None

        self.is_finished: bool = False
        self.is_error: bool = False
        self.last_status_message_timestamp: int = 0

    def on_update(self, status_message: ChannelStatusMessage):
        self.last_status_message_timestamp = status_message.timestamp

        if ChannelStatus.Opened == status_message.status:
            self.canvas.itemconfig(self.status_display_l, fill="yellow")
            self.canvas.itemconfig(self.status_display_r, fill="yellow")
        elif ChannelStatus.Started == status_message.status:
            self.canvas.itemconfig(self.status_display_l, fill="green")
            self.canvas.itemconfig(self.status_display_r, fill="green")
        elif ChannelStatus.Error == status_message.status:
            self.canvas.itemconfig(self.status_display_l, fill="red")
            self.canvas.itemconfig(self.status_display_r, fill="red")
            self.is_error = True
        elif ChannelStatus.Stopped == status_message.status:
            if not self.is_error:
                self.canvas.itemconfig(self.status_display_l, fill="blue")
                self.canvas.itemconfig(self.status_display_r, fill="blue")
        elif ChannelStatus.Closed == status_message.status:
            if not self.is_error:
                self.canvas.itemconfig(self.status_display_l, fill="lightgrey")
                self.canvas.itemconfig(self.status_display_r, fill="lightgrey")
            self.is_finished = True

        if status_message.payload is not None:
            if "sentRecordCount" in status_message.payload:
                self.canvas.itemconfig(self.channel_text, text=status_message.payload["sentRecordCount"])
            elif "receivedRecordCount" in status_message.payload:
                self.canvas.itemconfig(self.channel_text, text=status_message.payload["receivedRecordCount"])

    def draw(self, left, right):
        self.left = left
        self.top = right
        self.right = left + ViewConfig.channel_width
        self.bottom = right + ViewConfig.channel_cell_height

        self.channel_box = self.canvas.create_rectangle(self.left, self.top, self.right, self.bottom,
                                                        outline="", fill="white")
        self.channel_text = self.canvas.create_text(self.left + ViewConfig.channel_width / 2,
                                                    self.top + ViewConfig.channel_cell_height / 2,
                                                    text="0", fill="black", justify=tk.CENTER)

        self.status_display_l = self.canvas.create_rectangle(self.left, self.top, self.left + 10, self.bottom,
                                                             outline="", fill="gray")
        self.status_display_r = self.canvas.create_rectangle(self.right - 10, self.top, self.right, self.bottom,
                                                             outline="", fill="gray")


class PortView:
    def __init__(self, canvas: tk.Canvas, port_name: str):
        self.canvas = canvas

        self.port_name: str = port_name

        self.channel_views: list[ChannelRWView] = list()

        self.port_name_text = None
        self.port_box = None

        self.left = None
        self.top = None
        self.right = None
        self.bottom = None

    def draw(self, left, top):
        self.left = left
        self.top = top
        self.right = left + ViewConfig.channel_width
        self.bottom = top + 25 + len(self.channel_views) * ViewConfig.channel_cell_height

        self.port_box = self.canvas.create_rectangle(self.left, self.top, self.right, self.bottom,
                                                     outline="", fill="black")

        self.port_name_text = self.canvas.create_text(self.left + ViewConfig.channel_width / 2,
                                                      self.top + 8,
                                                      text=self.port_name, fill="white", justify=tk.CENTER)

        for idx, channel_view in enumerate(self.channel_views):
            channel_view.draw(self.left, self.top + 20 + idx * (ViewConfig.channel_cell_height + 1))


class OperatorView:

    def __init__(self, canvas: tk.Canvas, operator: Operator):
        self.canvas = canvas
        self.name: str = operator.get_simple_name()

        self.input_port_views: dict[ChannelInputPort, PortView] = {
            input_port: PortView(canvas, input_port.get_simple_name()) for input_port in
            operator.get_protected().get_nested_instances().values() if isinstance(input_port, ChannelInputPort)
        }

        self.output_port_views: dict[ChannelOutputPort, PortView] = {
            output_port: PortView(canvas, output_port.get_simple_name()) for output_port in
            operator.get_protected().get_nested_instances().values() if isinstance(output_port, ChannelOutputPort)
        }

        self.left = None
        self.top = None
        self.right = None
        self.bottom = None

    def draw(self, left, top):
        self.left = left
        self.top = top
        self.right = left + ViewConfig.operator_width

        operator_box = self.canvas.create_rectangle(self.left, self.top, self.right, self.top,
                                                    outline="", fill="grey40")
        self.canvas.create_text(self.left + ViewConfig.operator_width / 2, self.top + 10,
                                text=self.name, width=ViewConfig.operator_width, fill="white")

        input_y_pos = self.top + 25
        for input_port_view in self.input_port_views.values():
            input_port_view.draw(self.left, input_y_pos)
            input_y_pos = 5 + input_port_view.bottom

        output_y_pos = self.top + 25
        for output_port_view in self.output_port_views.values():
            output_port_view.draw(self.right - ViewConfig.channel_width,
                                  output_y_pos)
            output_y_pos = 5 + output_port_view.bottom

        self.bottom = max(input_y_pos, output_y_pos)

        self.canvas.coords(operator_box, self.left, self.top, self.right, self.bottom)
