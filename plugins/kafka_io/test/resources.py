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
from typing import Optional

from pypz.plugins.loggers.default import DefaultLoggerPlugin
from pypz.plugins.kafka_io.ports import KafkaChannelOutputPort, KafkaChannelInputPort
from pypz.core.commons.parameters import OptionalParameter
from pypz.core.specs.misc import BlankOperator
from pypz.core.specs.pipeline import Pipeline
from pypz.core.specs.instance import Instance, InstanceGroup

avro_schema_string = """
{
    "type": "record",
    "name": "DemoRecord",
    "fields": [
        {
            "name": "demoText",
            "type": "string"
        }
    ]
}
"""


class TestWriterOperator(BlankOperator):

    max_record_count = OptionalParameter(int, alt_name="maxRecordCount")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.output_port = KafkaChannelOutputPort(schema=avro_schema_string)
        self.group_output_port = KafkaChannelOutputPort(schema=avro_schema_string)
        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")

        self.max_record_count = 2
        self.record_count = 0

    def _on_init(self) -> bool:
        return True

    def _on_shutdown(self) -> bool:
        return True

    def _on_running(self) -> bool:
        self.output_port.send([{"demoText": f"record_{self.record_count}"}])

        self.record_count += 1
        if self.record_count == self.max_record_count:
            return True
        return False


class TestReaderOperator(BlankOperator):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.input_port = KafkaChannelInputPort(schema=avro_schema_string)
        self.group_input_port = KafkaChannelInputPort(schema=avro_schema_string, group_mode=True)
        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")
        self.received_records: list = list()

    def _on_init(self) -> bool:
        return True

    def _on_shutdown(self) -> bool:
        return True

    def _on_running(self) -> bool:
        records = self.input_port.retrieve()
        self.received_records.extend(records)
        for record in records:
            self.get_logger().debug(record)
        return not self.input_port.can_retrieve()


class TestPipeline(Pipeline):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.reader = TestReaderOperator()
        self.reader.set_parameter("replicationFactor", 1)
        self.writer = TestWriterOperator()
        self.writer.set_parameter("replicationFactor", 1)

        self.reader.input_port.connect(self.writer.output_port)
        self.reader.group_input_port.connect(self.writer.group_output_port)


class TestGroupInfo(InstanceGroup):

    def __init__(self, group_size: int = 1,
                 group_index: int = 0,
                 group_name: str = None,
                 group_principal: Instance = None,
                 is_principal: bool = None):
        self._group_size = group_size
        self._group_index = group_index
        self._group_name = group_name
        self._group_principal = group_principal
        self._is_principal = is_principal if is_principal is not None else (group_index == 0)

    def get_group_size(self) -> int:
        return self._group_size

    def get_group_index(self) -> int:
        return self._group_index

    def get_group_name(self) -> Optional[str]:
        return self._group_name

    def get_group_principal(self) -> Optional[Instance]:
        return self._group_principal

    def is_principal(self) -> bool:
        return self._is_principal
