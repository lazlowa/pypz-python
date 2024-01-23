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
from pypz.abstracts.misc import BlankChannelInputPort, BlankChannelOutputPort
from pypz.core.commons.parameters import OptionalParameter
from pypz.core.specs.misc import BlankOperator
from pypz.core.specs.pipeline import Pipeline

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


class TestOperator(BlankOperator):

    max_record_count = OptionalParameter(int, alt_name="maxRecordCount")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.input_port = BlankChannelInputPort(schema=avro_schema_string)
        self.input_port_2 = BlankChannelInputPort(schema=avro_schema_string)
        self.output_port = BlankChannelOutputPort(schema=avro_schema_string)
        self.output_port_2 = BlankChannelOutputPort(schema=avro_schema_string)


class TestPipelineWithSimpleConnections(Pipeline):

    def __init__(self, name: str = None, replication_factor=0, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.op_a = TestOperator()
        self.op_b = TestOperator()
        self.op_c = TestOperator()
        self.op_d = TestOperator()

        self.op_a.set_parameter("replicationFactor", replication_factor)
        self.op_b.set_parameter("replicationFactor", replication_factor)
        self.op_c.set_parameter("replicationFactor", replication_factor)
        self.op_d.set_parameter("replicationFactor", replication_factor)

        self.op_d.input_port.connect(self.op_c.output_port)
        self.op_c.input_port.connect(self.op_b.output_port)
        self.op_b.input_port.connect(self.op_a.output_port)


class TestPipelineWithBranchingConnections(Pipeline):

    def __init__(self, name: str = None, replication_factor=0, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.op_a = TestOperator()
        self.op_b = TestOperator()
        self.op_c = TestOperator()
        self.op_d = TestOperator()
        self.op_e = TestOperator()
        self.op_f = TestOperator()
        self.op_g = TestOperator()

        self.op_a.set_parameter("replicationFactor", replication_factor)
        self.op_b.set_parameter("replicationFactor", replication_factor)
        self.op_c.set_parameter("replicationFactor", replication_factor)
        self.op_d.set_parameter("replicationFactor", replication_factor)
        self.op_e.set_parameter("replicationFactor", replication_factor)
        self.op_f.set_parameter("replicationFactor", replication_factor)
        self.op_g.set_parameter("replicationFactor", replication_factor)

        self.op_a.output_port.connect(self.op_b.input_port)
        self.op_a.output_port.connect(self.op_c.input_port)

        self.op_b.output_port.connect(self.op_d.input_port)
        self.op_b.output_port.connect(self.op_e.input_port)

        self.op_c.output_port.connect(self.op_f.input_port)
        self.op_c.output_port.connect(self.op_g.input_port)


class TestPipelineWithMergingConnections(Pipeline):

    def __init__(self, name: str = None, replication_factor=0, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.op_a = TestOperator()
        self.op_b = TestOperator()
        self.op_c = TestOperator()
        self.op_d = TestOperator()
        self.op_e = TestOperator()
        self.op_f = TestOperator()
        self.op_g = TestOperator()

        self.op_a.set_parameter("replicationFactor", replication_factor)
        self.op_b.set_parameter("replicationFactor", replication_factor)
        self.op_c.set_parameter("replicationFactor", replication_factor)
        self.op_d.set_parameter("replicationFactor", replication_factor)
        self.op_e.set_parameter("replicationFactor", replication_factor)
        self.op_f.set_parameter("replicationFactor", replication_factor)
        self.op_g.set_parameter("replicationFactor", replication_factor)

        self.op_a.input_port.connect(self.op_b.output_port)
        self.op_a.input_port.connect(self.op_c.output_port)

        self.op_b.input_port.connect(self.op_d.output_port)
        self.op_b.input_port.connect(self.op_e.output_port)

        self.op_c.input_port.connect(self.op_f.output_port)
        self.op_c.input_port.connect(self.op_g.output_port)


class TestPipelineWithBranchingAndMergingConnections(Pipeline):

    def __init__(self, name: str = None, replication_factor=0, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.op_a = TestOperator()
        self.op_b = TestOperator()
        self.op_c = TestOperator()
        self.op_d = TestOperator()
        self.op_e = TestOperator()
        self.op_f = TestOperator()
        self.op_g = TestOperator()
        self.op_h = TestOperator()

        self.op_a.set_parameter("replicationFactor", replication_factor)
        self.op_b.set_parameter("replicationFactor", replication_factor)
        self.op_c.set_parameter("replicationFactor", replication_factor)
        self.op_d.set_parameter("replicationFactor", replication_factor)
        self.op_e.set_parameter("replicationFactor", replication_factor)
        self.op_f.set_parameter("replicationFactor", replication_factor)
        self.op_g.set_parameter("replicationFactor", replication_factor)
        self.op_h.set_parameter("replicationFactor", replication_factor)

        self.op_a.output_port.connect(self.op_c.input_port)
        self.op_b.output_port.connect(self.op_c.input_port)

        self.op_c.output_port.connect(self.op_d.input_port)
        self.op_c.output_port.connect(self.op_e.input_port)

        self.op_d.output_port.connect(self.op_f.input_port)
        self.op_e.output_port.connect(self.op_f.input_port)

        self.op_f.output_port.connect(self.op_g.input_port)
        self.op_f.output_port.connect(self.op_h.input_port)


class TestPipelineWithBranchingAndMergingConnectionsWithMultipleOutputs(Pipeline):

    def __init__(self, name: str = None, replication_factor=0, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.op_a = TestOperator()
        self.op_b = TestOperator()
        self.op_c = TestOperator()
        self.op_d = TestOperator()
        self.op_e = TestOperator()
        self.op_f = TestOperator()
        self.op_g = TestOperator()
        self.op_h = TestOperator()

        self.op_a.set_parameter("replicationFactor", replication_factor)
        self.op_b.set_parameter("replicationFactor", replication_factor)
        self.op_c.set_parameter("replicationFactor", replication_factor)
        self.op_d.set_parameter("replicationFactor", replication_factor)
        self.op_e.set_parameter("replicationFactor", replication_factor)
        self.op_f.set_parameter("replicationFactor", replication_factor)
        self.op_g.set_parameter("replicationFactor", replication_factor)
        self.op_h.set_parameter("replicationFactor", replication_factor)

        self.op_a.output_port.connect(self.op_c.input_port)
        self.op_b.output_port.connect(self.op_c.input_port)
        self.op_c.output_port.connect(self.op_d.input_port)
        self.op_c.output_port.connect(self.op_e.input_port)
        self.op_d.output_port.connect(self.op_f.input_port)
        self.op_e.output_port.connect(self.op_f.input_port)
        self.op_f.output_port.connect(self.op_g.input_port)
        self.op_f.output_port.connect(self.op_h.input_port)

        self.op_a.output_port_2.connect(self.op_c.input_port)
        self.op_b.output_port_2.connect(self.op_c.input_port)
        self.op_c.output_port_2.connect(self.op_d.input_port)
        self.op_c.output_port_2.connect(self.op_e.input_port)
        self.op_d.output_port_2.connect(self.op_f.input_port)
        self.op_e.output_port_2.connect(self.op_f.input_port)
        self.op_f.output_port_2.connect(self.op_g.input_port)
        self.op_f.output_port_2.connect(self.op_h.input_port)


class TestPipelineWithBranchingAndMergingConnectionsWithAdditionalOutputs(Pipeline):

    def __init__(self, name: str = None, replication_factor=0, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.op_a = TestOperator()
        self.op_b = TestOperator()
        self.op_c = TestOperator()
        self.op_d = TestOperator()
        self.op_e = TestOperator()
        self.op_f = TestOperator()
        self.op_g = TestOperator()
        self.op_h = TestOperator()

        self.op_a.set_parameter("replicationFactor", replication_factor)
        self.op_b.set_parameter("replicationFactor", replication_factor)
        self.op_c.set_parameter("replicationFactor", replication_factor)
        self.op_d.set_parameter("replicationFactor", replication_factor)
        self.op_e.set_parameter("replicationFactor", replication_factor)
        self.op_f.set_parameter("replicationFactor", replication_factor)
        self.op_g.set_parameter("replicationFactor", replication_factor)
        self.op_h.set_parameter("replicationFactor", replication_factor)

        self.op_a.output_port.connect(self.op_c.input_port)
        self.op_b.output_port.connect(self.op_c.input_port)
        self.op_c.output_port.connect(self.op_d.input_port)
        self.op_d.output_port.connect(self.op_f.input_port)
        self.op_e.output_port.connect(self.op_f.input_port)
        self.op_f.output_port.connect(self.op_g.input_port)

        self.op_c.output_port_2.connect(self.op_e.input_port)
        self.op_f.output_port_2.connect(self.op_h.input_port)


class TestPipelineWithCircularDependentOperators(Pipeline):

    def __init__(self, name: str = None, replication_factor=0, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.op_a = TestOperator()
        self.op_b = TestOperator()
        self.op_c = TestOperator()
        self.op_d = TestOperator()

        self.op_a.set_parameter("replicationFactor", replication_factor)
        self.op_b.set_parameter("replicationFactor", replication_factor)
        self.op_c.set_parameter("replicationFactor", replication_factor)
        self.op_d.set_parameter("replicationFactor", replication_factor)

        self.op_d.input_port.connect(self.op_c.output_port)
        self.op_c.input_port.connect(self.op_b.output_port)
        self.op_b.input_port.connect(self.op_a.output_port)
        self.op_a.input_port.connect(self.op_d.output_port)
