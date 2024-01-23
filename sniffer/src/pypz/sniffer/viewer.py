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
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, Future

from pypz.sniffer.sniffer import PipelineSniffer
from pypz.sniffer.utils import order_operators_by_connections
from pypz.sniffer.views import OperatorView, ChannelView, ChannelRWView, ViewConfig
from pypz.abstracts.channel_ports import ChannelInputPort, ChannelOutputPort
from pypz.core.specs.dtos import PipelineInstanceDTO
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline


class PipelineSnifferViewer(tk.Tk):

    @staticmethod
    def create_from_string(source: str):
        return PipelineSnifferViewer(Pipeline.create_from_string(source, mock_nonexistent=True))

    @staticmethod
    def create_from_dto(instance_dto: PipelineInstanceDTO):
        return PipelineSnifferViewer(Pipeline.create_from_dto(instance_dto, mock_nonexistent=True))

    def __init__(self, pipeline: Pipeline):
        super().__init__(screenName=pipeline.get_full_name())

        if not isinstance(pipeline, Pipeline):
            raise TypeError(f"Invalid argument type: {type(pipeline)}; Expected: {Pipeline}")

        self._pipeline: Pipeline = pipeline
        self._pipeline_sniffer: PipelineSniffer = PipelineSniffer(pipeline)
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor()
        self._sniffer_futures: set[Future] = set()
        self._operator_views: dict[Operator, OperatorView] = dict()
        self._channel_views: dict[str, ChannelView] = dict()
        self._dependency_levels: list[set[Operator]] = order_operators_by_connections(pipeline)
        self._operator_view_channel_reader_count: dict[Operator, int] = dict()
        self._operator_view_channel_writer_count: dict[Operator, int] = dict()
        self._stopped: bool = False
        self._shutdown: bool = False
        self._start_time: int = 0

        # Initialize graphical elements
        # =============================

        self.title(f"Pipeline Sniffer ({pipeline.get_full_name()})")
        self.geometry("1200x600")
        self.minsize(600, 200)

        self._frame = tk.Frame(self)
        self._frame.pack(fill=tk.BOTH, expand=True)

        self._pipeline_canvas = tk.Canvas(self._frame, background="WhiteSmoke")
        self._pipeline_canvas.grid(row=0, column=0, sticky="nsew")

        self._hsb = tk.Scrollbar(self._frame, orient="horizontal", command=self._pipeline_canvas.xview)
        self._hsb.grid(row=1, column=0, sticky="ew")

        self._vsb = tk.Scrollbar(self._frame, orient="vertical", command=self._pipeline_canvas.yview)
        self._vsb.grid(row=0, column=1, sticky="ns")

        self._pipeline_canvas.configure(xscrollcommand=self._hsb.set, yscrollcommand=self._vsb.set)
        self._pipeline_canvas.bind("<Configure>", self.__on_canvas_configure)

        self._status_frame = tk.Frame(self._frame, height=50)
        self._status_frame.grid(row=2, column=0, sticky="nsew")

        self._sniffer_status = tk.Label(self._status_frame, text="Status: Waiting", font=("Helvetica", 12, "bold"))
        self._sniffer_status.grid(row=0, column=0, sticky="w")
        self._status_frame.columnconfigure(0, pad=10)
        self._elapsed_time_display = tk.Label(self._status_frame, text="Elapsed time: 0 [sec]",
                                              font=("Helvetica", 12, "bold"))
        self._elapsed_time_display.grid(row=0, column=1, sticky="nsw")

        self._frame.columnconfigure(0, weight=1)
        self._frame.rowconfigure(0, weight=1)

        self.wm_protocol("WM_DELETE_WINDOW", self.__exit)

        self.__init_sniffer()

    def __on_canvas_configure(self, event):
        self._pipeline_canvas.configure(scrollregion=self._pipeline_canvas.bbox("all"))

    def __init_sniffer(self):
        for operator in self._pipeline.get_protected().get_nested_instances().values():
            self._operator_views[operator] = OperatorView(self._pipeline_canvas, operator)

        for operator, operator_view in self._operator_views.items():
            for input_port, input_port_view in operator_view.input_port_views.items():
                for output_port in input_port.get_connected_ports():
                    if isinstance(output_port, ChannelOutputPort):
                        channel_id = PipelineSniffer.get_channel_id(input_port, output_port)

                        if channel_id not in self._channel_views:
                            if operator not in self._operator_view_channel_reader_count:
                                self._operator_view_channel_reader_count[operator] = 0
                            if output_port.get_context() not in self._operator_view_channel_writer_count:
                                self._operator_view_channel_writer_count[output_port.get_context()] = 0

                            self._channel_views[channel_id] = \
                                ChannelView(self._pipeline_canvas,
                                            self._operator_view_channel_reader_count[operator],
                                            self._operator_view_channel_writer_count[output_port.get_context()])
                            self._operator_view_channel_reader_count[operator] += 1
                            self._operator_view_channel_writer_count[output_port.get_context()] += 1

                        channel_reader_view = ChannelRWView(self._pipeline_canvas)
                        self._pipeline_sniffer.channel_sniffers[channel_id].subscribe(input_port.get_full_name(),
                                                                                      channel_reader_view.on_update)
                        input_port_view.channel_views.append(channel_reader_view)
                        self._channel_views[channel_id].channel_reader_views.append(channel_reader_view)

            for output_port, output_port_view in operator_view.output_port_views.items():
                for input_port in output_port.get_connected_ports():
                    if isinstance(input_port, ChannelInputPort):
                        channel_id = PipelineSniffer.get_channel_id(input_port, output_port)

                        if channel_id not in self._channel_views:
                            if operator not in self._operator_view_channel_writer_count:
                                self._operator_view_channel_writer_count[operator] = 0
                            if input_port.get_context() not in self._operator_view_channel_reader_count:
                                self._operator_view_channel_reader_count[input_port.get_context()] = 0

                            self._channel_views[channel_id] = \
                                ChannelView(self._pipeline_canvas,
                                            self._operator_view_channel_reader_count[input_port.get_context()],
                                            self._operator_view_channel_writer_count[operator])
                            self._operator_view_channel_writer_count[operator] += 1
                            self._operator_view_channel_reader_count[input_port.get_context()] += 1

                        channel_writer_view = ChannelRWView(self._pipeline_canvas)
                        self._pipeline_sniffer.channel_sniffers[channel_id].subscribe(output_port.get_full_name(),
                                                                                      channel_writer_view.on_update)
                        output_port_view.channel_views.append(channel_writer_view)
                        self._channel_views[channel_id].channel_writer_views.append(channel_writer_view)

        next_y = ViewConfig.operator_start_pos_y
        for idx, level in enumerate(self._dependency_levels):
            next_y = ViewConfig.operator_start_pos_y
            for operator in level:
                self._operator_views[operator].draw(ViewConfig.operator_start_pos_x + idx *
                                                    (ViewConfig.operator_width + ViewConfig.operator_spacing_hor),
                                                    next_y)
                next_y = ViewConfig.replica_spacing_ver + self._operator_views[operator].bottom

                for replica in operator.get_replicas():
                    self._operator_views[replica].draw(ViewConfig.operator_start_pos_x + idx *
                                                       (ViewConfig.operator_width + ViewConfig.operator_spacing_hor),
                                                       next_y)
                    next_y = ViewConfig.replica_spacing_ver + self._operator_views[replica].bottom

                next_y += ViewConfig.operator_spacing_ver

        for channel_view in self._channel_views.values():
            next_y = channel_view.draw(next_y)

        self.after(100, self.__start_sniffer)

    def __start_sniffer(self):
        if self._shutdown:
            self._sniffer_status.config(text="Status: Stopping")
            self.after(0, self.__stop_sniffer)
            return

        if self._pipeline_sniffer.start():
            self._start_time = time.time()
            self._sniffer_status.config(text="Status: Running")
            self.after(100, self.__sniff)
        else:
            self.after(1000, self.__start_sniffer)

    def __sniff(self):
        self._elapsed_time_display.config(text=f"Elapsed time: {round(time.time() - self._start_time)} [sec]")

        for future in self._sniffer_futures:
            if future.cancelled():
                continue
            if not future.done():
                self.after(1000, self.__sniff)
                return

        if self._shutdown:
            self._sniffer_status.config(text="Status: Stopping")
            self.after(0, self.__stop_sniffer)
            return

        all_finished = True
        for channel_view in self._channel_views.values():
            for channel_reader_view in channel_view.channel_reader_views:
                if not channel_reader_view.is_finished:
                    all_finished = False
                    break
            for channel_writer_view in channel_view.channel_writer_views:
                if not channel_writer_view.is_finished:
                    all_finished = False
                    break
            if not all_finished:
                break

        if all_finished:
            self.after(0, self.__stop_sniffer)
        else:
            self._sniffer_futures = {self._executor.submit(channel_sniffer.sniff) for
                                     channel_sniffer in self._pipeline_sniffer.channel_sniffers.values()}
            self.after(1000, self.__sniff)

    def __stop_sniffer(self):
        if self._pipeline_sniffer.stop():
            if not self._stopped:
                self._sniffer_status.config(text="Status: Stopped")
                self._stopped = True

            if self._shutdown:
                self.quit()

        self.after(500, self.__stop_sniffer)

    def __exit(self):
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._shutdown = True
