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
from typing import Any, Optional

from pypz.abstracts.channel_ports import ChannelInputPort, ChannelOutputPort
from pypz.core.specs.misc import BlankOperator
from pypz.core.specs.pipeline import Pipeline
from pypz.plugins.loggers.default import DefaultLoggerPlugin

from core.test.channels_tests.resources import TestChannelReader, TestChannelWriter


class TestChannelInputPort(ChannelInputPort):

    def __init__(
        self,
        name: str = None,
        schema: Any = None,
        group_mode: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(name, schema, group_mode, TestChannelReader, *args, **kwargs)


class TestChannelOutputPort(ChannelOutputPort):

    def __init__(self, name: str = None, schema: Optional[Any] = None, *args, **kwargs):
        super().__init__(name, schema, TestChannelWriter, *args, **kwargs)


class TestWriterOperator(BlankOperator):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.output_port = TestChannelOutputPort()
        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")


class TestReaderOperator(BlankOperator):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.input_port_a = TestChannelInputPort(None, "a")
        self.input_port_b = TestChannelInputPort(schema="b")
        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")


class TestPipeline(Pipeline):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.writer = TestWriterOperator()
        self.reader = TestReaderOperator()
        self.reader.input_port_a.connect(self.writer.output_port)
        self.reader.input_port_b.connect(self.writer.output_port)


class TestPipelineWithReplicatedOperators(Pipeline):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.writer = TestWriterOperator()
        self.writer.set_parameter("replicationFactor", 2)

        self.reader = TestReaderOperator()
        self.reader.set_parameter("replicationFactor", 2)

        self.reader.input_port_a.connect(self.writer.output_port)
        self.reader.input_port_b.connect(self.writer.output_port)
