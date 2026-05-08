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

from pypz.core.specs.dtos import (
    OperatorConnection,
    OperatorConnectionSource,
    OperatorInstanceDTO,
)
from pypz.core.specs.instance import ReplicaContext
from pypz.core.specs.utils import Internals

from core.test.specs_tests.operator_test_resources import (
    OperatorWithWrongLoggerPlugin,
    TestOperatorWithPortPlugins,
    TestPipelineWithOperator,
)


class OperatorInstanceTest(unittest.TestCase):

    def test_operator_dto_generation_with_connections_and_replication(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_a_dto = pipeline.operator_a.get_dto()

        self.assertEqual(5, operator_a_dto.parameters["replicationFactor"])
        self.assertEqual(
            [
                OperatorConnection(
                    "input_port", OperatorConnectionSource("operator_b", "output_port")
                )
            ],
            operator_a_dto.connections,
        )

    def test_operator_update_with_valid_input_types(self):
        pipeline = TestPipelineWithOperator("pipeline")

        try:
            pipeline.operator_a.update("{}")
            pipeline.operator_a.update({})
        except:  # noqa: E722
            self.fail()

    def test_operator_update_with_invalid_input_types(self):
        pipeline = TestPipelineWithOperator("pipeline")

        with self.assertRaises(TypeError):
            pipeline.operator_a.update(0)

        with self.assertRaises(TypeError):
            pipeline.operator_a.update(object())

    def test_operator_update_with_valid_connections(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[
                OperatorConnection(
                    "input_port", OperatorConnectionSource("operator_a", "output_port")
                )
            ]
        )

        pipeline.operator_b.update(operator_dto)

        self.assertEqual(1, len(pipeline.operator_b.input_port.get_connected_ports()))
        self.assertTrue(
            pipeline.operator_a.output_port
            in pipeline.operator_b.input_port.get_connected_ports()
        )

    def test_operator_update_multiple_with_valid_connections_expect_no_change(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[
                OperatorConnection(
                    "input_port", OperatorConnectionSource("operator_a", "output_port")
                )
            ]
        )

        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)

        self.assertEqual(1, len(pipeline.operator_b.input_port.get_connected_ports()))
        self.assertTrue(
            pipeline.operator_a.output_port
            in pipeline.operator_b.input_port.get_connected_ports()
        )

    def test_operator_update_with_non_existing_input_port_expect_error(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[
                OperatorConnection(
                    "non_existing",
                    OperatorConnectionSource("operator_b", "output_port"),
                )
            ]
        )

        with self.assertRaises(AttributeError):
            pipeline.operator_a.update(operator_dto)

    def test_operator_update_with_non_existing_source_instance_expect_error(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[
                OperatorConnection(
                    "input_port",
                    OperatorConnectionSource("non_existing", "output_port"),
                )
            ]
        )

        with self.assertRaises(AttributeError):
            pipeline.operator_a.update(operator_dto)

    def test_operator_update_with_non_existing_output_port_expect_error(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[
                OperatorConnection(
                    "input_port", OperatorConnectionSource("operator_b", "non_existing")
                )
            ]
        )

        with self.assertRaises(AttributeError):
            pipeline.operator_a.update(operator_dto)

    def test_operator_incremental_replication(self):
        pipeline = TestPipelineWithOperator("pipeline")
        self.assertEqual(5, pipeline.operator_a.get_replication_factor())
        self.assertEqual(6, pipeline.operator_a.get_group_size())
        self.assertEqual(5, len(pipeline.operator_a.get_replicas()))
        self.assertEqual(pipeline.operator_a, pipeline.operator_a.get_group_principal())

        pipeline.operator_a.set_parameter("replicationFactor", 1)

        self.assertEqual(1, pipeline.operator_a.get_replication_factor())
        self.assertEqual(2, pipeline.operator_a.get_group_size())
        self.assertEqual(1, len(pipeline.operator_a.get_replicas()))
        self.assertEqual(pipeline.operator_a, pipeline.operator_a.get_group_principal())

        pipeline.operator_a.set_parameter("replicationFactor", 5)

        self.assertEqual(5, pipeline.operator_a.get_replication_factor())
        self.assertEqual(6, pipeline.operator_a.get_group_size())
        self.assertEqual(5, len(pipeline.operator_a.get_replicas()))
        self.assertEqual(pipeline.operator_a, pipeline.operator_a.get_group_principal())

        pipeline.operator_a.set_parameter("replicationFactor", 10)

        self.assertEqual(10, pipeline.operator_a.get_replication_factor())
        self.assertEqual(11, pipeline.operator_a.get_group_size())
        self.assertEqual(10, len(pipeline.operator_a.get_replicas()))
        self.assertEqual(pipeline.operator_a, pipeline.operator_a.get_group_principal())

        pipeline.operator_a.set_parameter("replicationFactor", 0)

        self.assertEqual(0, pipeline.operator_a.get_replication_factor())
        self.assertEqual(1, pipeline.operator_a.get_group_size())
        self.assertEqual(0, len(pipeline.operator_a.get_replicas()))
        self.assertIsNone(pipeline.operator_a.get_group_principal())

    def test_operator_replication_basic_attributes(self):
        pipeline = TestPipelineWithOperator("pipeline")

        self.assertEqual(5, pipeline.operator_a.get_replication_factor())
        self.assertEqual(6, pipeline.operator_a.get_group_size())
        self.assertEqual(0, pipeline.operator_a.get_group_index())
        self.assertEqual(5, len(pipeline.operator_a.get_replicas()))
        self.assertEqual(pipeline.operator_a, pipeline.operator_a.get_group_principal())
        self.assertEqual(
            pipeline.operator_a.get_full_name(), pipeline.operator_a.get_group_name()
        )
        self.assertTrue(pipeline.operator_a.is_principal())
        self.assertEqual("pipeline.operator_a", pipeline.operator_a.get_full_name())

        self.assertEqual(6, pipeline.operator_a.output_port.get_group_size())
        self.assertEqual(0, pipeline.operator_a.output_port.get_group_index())
        self.assertEqual(
            pipeline.operator_a.output_port,
            pipeline.operator_a.output_port.get_group_principal(),
        )
        self.assertEqual(
            pipeline.operator_a.output_port.get_full_name(),
            pipeline.operator_a.output_port.get_group_name(),
        )
        self.assertTrue(pipeline.operator_a.output_port.is_principal())

        self.assertEqual(6, pipeline.operator_a.input_port.get_group_size())
        self.assertEqual(0, pipeline.operator_a.input_port.get_group_index())
        self.assertEqual(
            pipeline.operator_a.input_port,
            pipeline.operator_a.input_port.get_group_principal(),
        )
        self.assertEqual(
            pipeline.operator_a.input_port.get_full_name(),
            pipeline.operator_a.input_port.get_group_name(),
        )
        self.assertTrue(pipeline.operator_a.input_port.is_principal())

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(6, replica.get_group_size())
            self.assertEqual(replica_idx + 1, replica.get_group_index())
            self.assertEqual(pipeline.operator_a, replica.get_group_principal())
            self.assertEqual(
                pipeline.operator_a.get_full_name(), replica.get_group_name()
            )
            self.assertFalse(replica.is_principal())
            self.assertEqual(
                f"pipeline.operator_a_{replica_idx}", replica.get_full_name()
            )

            self.assertEqual(6, replica.output_port.get_group_size())
            self.assertEqual(replica_idx + 1, replica.output_port.get_group_index())
            self.assertEqual(
                pipeline.operator_a.output_port,
                replica.output_port.get_group_principal(),
            )
            self.assertEqual(
                pipeline.operator_a.output_port.get_full_name(),
                replica.output_port.get_group_name(),
            )
            self.assertFalse(replica.output_port.is_principal())
            self.assertEqual(
                f"pipeline.operator_a_{replica_idx}.output_port",
                replica.output_port.get_full_name(),
            )

            self.assertEqual(6, replica.input_port.get_group_size())
            self.assertEqual(replica_idx + 1, replica.input_port.get_group_index())
            self.assertEqual(
                pipeline.operator_a.input_port, replica.input_port.get_group_principal()
            )
            self.assertEqual(
                pipeline.operator_a.input_port.get_full_name(),
                replica.input_port.get_group_name(),
            )
            self.assertFalse(replica.input_port.is_principal())
            self.assertEqual(
                f"pipeline.operator_a_{replica_idx}.input_port",
                replica.input_port.get_full_name(),
            )

    def test_operator_replication_direct_set_parameter_attribute_expect_equality(self):
        pipeline = TestPipelineWithOperator("pipeline")

        self.assertEqual(0, pipeline.operator_a.param_a)
        self.assertEqual(0, pipeline.operator_a.param_b)
        self.assertEqual(0, pipeline.operator_a.param_c)
        self.assertEqual(0, pipeline.operator_a.param_d)
        self.assertEqual(0, pipeline.operator_a.param_e)
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_a"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_b"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_c"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_d"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_e"))

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertIs(
                Internals(pipeline.operator_a).parameters,
                Internals(replica).parameters,
            )

        pipeline.operator_a.param_a = 1
        pipeline.operator_a.param_b = 12
        pipeline.operator_a.param_c = 123
        pipeline.operator_a.param_d = 1234
        pipeline.operator_a.param_e = 12345

        self.assertEqual(1, pipeline.operator_a.param_a)
        self.assertEqual(12, pipeline.operator_a.param_b)
        self.assertEqual(123, pipeline.operator_a.param_c)
        self.assertEqual(1234, pipeline.operator_a.param_d)
        self.assertEqual(12345, pipeline.operator_a.param_e)
        self.assertEqual(1, pipeline.operator_a.get_parameter("param_a"))
        self.assertEqual(12, pipeline.operator_a.get_parameter("param_b"))
        self.assertEqual(123, pipeline.operator_a.get_parameter("param_c"))
        self.assertEqual(1234, pipeline.operator_a.get_parameter("param_d"))
        self.assertEqual(12345, pipeline.operator_a.get_parameter("param_e"))

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertIs(
                Internals(pipeline.operator_a).parameters,
                Internals(replica).parameters,
            )

        pipeline.operator_a.get_replica(0).param_a = 12345
        pipeline.operator_a.get_replica(0).param_b = 1234
        pipeline.operator_a.get_replica(0).param_c = 123
        pipeline.operator_a.get_replica(0).param_d = 12
        pipeline.operator_a.get_replica(0).param_e = 1

        self.assertEqual(12345, pipeline.operator_a.param_a)
        self.assertEqual(1234, pipeline.operator_a.param_b)
        self.assertEqual(123, pipeline.operator_a.param_c)
        self.assertEqual(12, pipeline.operator_a.param_d)
        self.assertEqual(1, pipeline.operator_a.param_e)
        self.assertEqual(12345, pipeline.operator_a.get_parameter("param_a"))
        self.assertEqual(1234, pipeline.operator_a.get_parameter("param_b"))
        self.assertEqual(123, pipeline.operator_a.get_parameter("param_c"))
        self.assertEqual(12, pipeline.operator_a.get_parameter("param_d"))
        self.assertEqual(1, pipeline.operator_a.get_parameter("param_e"))

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertIs(
                Internals(pipeline.operator_a).parameters,
                Internals(replica).parameters,
            )

    def test_operator_replication_indirect_set_parameter_attribute_expect_equality(
        self,
    ):
        pipeline = TestPipelineWithOperator("pipeline")

        self.assertEqual(0, pipeline.operator_a.param_a)
        self.assertEqual(0, pipeline.operator_a.param_b)
        self.assertEqual(0, pipeline.operator_a.param_c)
        self.assertEqual(0, pipeline.operator_a.param_d)
        self.assertEqual(0, pipeline.operator_a.param_e)
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_a"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_b"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_c"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_d"))
        self.assertEqual(0, pipeline.operator_a.get_parameter("param_e"))

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertIs(
                Internals(pipeline.operator_a).parameters,
                Internals(replica).parameters,
            )

        pipeline.operator_a.set_parameter("param_a", 1)
        pipeline.operator_a.set_parameter("param_b", 12)
        pipeline.operator_a.set_parameter("param_c", 123)
        pipeline.operator_a.set_parameter("param_d", 1234)
        pipeline.operator_a.set_parameter("param_e", 12345)

        self.assertEqual(1, pipeline.operator_a.param_a)
        self.assertEqual(12, pipeline.operator_a.param_b)
        self.assertEqual(123, pipeline.operator_a.param_c)
        self.assertEqual(1234, pipeline.operator_a.param_d)
        self.assertEqual(12345, pipeline.operator_a.param_e)
        self.assertEqual(1, pipeline.operator_a.get_parameter("param_a"))
        self.assertEqual(12, pipeline.operator_a.get_parameter("param_b"))
        self.assertEqual(123, pipeline.operator_a.get_parameter("param_c"))
        self.assertEqual(1234, pipeline.operator_a.get_parameter("param_d"))
        self.assertEqual(12345, pipeline.operator_a.get_parameter("param_e"))

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertIs(
                Internals(pipeline.operator_a).parameters,
                Internals(replica).parameters,
            )

        pipeline.operator_a.get_replica(0).set_parameter("param_a", 12345)
        pipeline.operator_a.get_replica(0).set_parameter("param_b", 1234)
        pipeline.operator_a.get_replica(0).set_parameter("param_c", 123)
        pipeline.operator_a.get_replica(0).set_parameter("param_d", 12)
        pipeline.operator_a.get_replica(0).set_parameter("param_e", 1)

        self.assertEqual(12345, pipeline.operator_a.param_a)
        self.assertEqual(1234, pipeline.operator_a.param_b)
        self.assertEqual(123, pipeline.operator_a.param_c)
        self.assertEqual(12, pipeline.operator_a.param_d)
        self.assertEqual(1, pipeline.operator_a.param_e)
        self.assertEqual(12345, pipeline.operator_a.get_parameter("param_a"))
        self.assertEqual(1234, pipeline.operator_a.get_parameter("param_b"))
        self.assertEqual(123, pipeline.operator_a.get_parameter("param_c"))
        self.assertEqual(12, pipeline.operator_a.get_parameter("param_d"))
        self.assertEqual(1, pipeline.operator_a.get_parameter("param_e"))

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertIs(
                Internals(pipeline.operator_a).parameters,
                Internals(replica).parameters,
            )

    def test_operator_replication_basic_attributes_without_pipeline_context(self):
        operator = TestOperatorWithPortPlugins("operator")
        operator.set_parameter("replicationFactor", 5)

        self.assertEqual(5, operator.get_replication_factor())
        self.assertEqual(5, len(operator.get_replicas()))
        self.assertEqual(operator, operator.get_group_principal())
        self.assertEqual(operator.get_simple_name(), operator.get_group_name())
        self.assertTrue(operator.is_principal())
        self.assertEqual("operator", operator.get_full_name())

        for replica_idx in range(operator.get_replication_factor()):
            replica = operator.get_replica(replica_idx)

            self.assertEqual(operator, replica.get_group_principal())
            self.assertEqual(operator.get_simple_name(), replica.get_group_name())
            self.assertFalse(replica.is_principal())
            self.assertEqual(f"operator_{replica_idx}", replica.get_full_name())

    def test_operator_replication_replica_equality_without_updates(self):
        operator = TestOperatorWithPortPlugins("operator", 5)
        operator_internals = Internals(operator)

        for replica in operator.get_replicas():
            replica_internals = Internals(operator)

            self.assertEqual(5, replica.get_replication_factor())
            self.assertEqual(id(operator), id(replica.get_group_principal()))
            self.assertEqual(0, len(replica.get_replicas()))
            self.assertEqual(id(operator.get_context()), id(replica.get_context()))
            self.assertEqual(
                operator_internals.nested_instances,
                replica_internals.nested_instances,
            )
            self.assertEqual(
                operator_internals.parameters,
                replica_internals.parameters,
            )
            self.assertEqual(
                operator_internals.depends_on,
                replica_internals.depends_on,
            )

    def test_operator_replication_replica_equality_with_updates_on_origin(self):
        operator = TestOperatorWithPortPlugins("operator", 1)
        operator.set_parameter("replicationFactor", 1)

        operator.set_parameter("param", "value")
        operator.output_port.depends_on(operator.input_port)
        operator.input_port.set_parameter("param", "value")
        operator.output_port.set_parameter("req_str", "val")
        operator.output_port.set_parameter("_opt_str", "val")
        operator.output_port.set_parameter("req_int", 4321)
        operator.output_port.set_parameter("_opt_int", 4321)

        for replica in operator.get_replicas():
            self.assertTrue(replica.is_equivalent_to(operator))
            for name, nested in Internals(replica).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(operator).nested_instances[name]
                ):
                    self.fail()
            for name, nested in Internals(operator).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(replica).nested_instances[name]
                ):
                    self.fail()
            self.assertIs(
                Internals(operator).parameters,
                Internals(replica).parameters,
            )
            self.assertIs(
                Internals(operator).depends_on,
                Internals(replica).depends_on,
            )
            self.assertIs(
                Internals(operator.output_port).parameters,
                Internals(replica.output_port).parameters,
            )
            self.assertIs(
                Internals(operator.output_port).depends_on,
                Internals(replica.output_port).depends_on,
            )
            self.assertIs(
                Internals(operator.input_port).parameters,
                Internals(replica.input_port).parameters,
            )
            self.assertIs(
                Internals(operator.input_port).depends_on,
                Internals(replica.input_port).depends_on,
            )
            self.assertIs(operator.output_port.req_str, replica.output_port.req_str)
            self.assertIs(operator.output_port._opt_str, replica.output_port._opt_str)
            self.assertIs(operator.output_port.req_int, replica.output_port.req_int)
            self.assertIs(operator.output_port._opt_int, replica.output_port._opt_int)

    def test_operator_replication_replica_equality_with_updates_on_replica(self):
        operator = TestOperatorWithPortPlugins("operator")
        operator.set_parameter("replicationFactor", 1)

        operator.get_replica(0).set_parameter("param", "value")
        operator.get_replica(0).output_port.depends_on(
            operator.get_replica(0).input_port
        )
        operator.get_replica(0).input_port.set_parameter("param", "value")
        operator.get_replica(0).output_port.set_parameter("req_str", "val")
        operator.get_replica(0).output_port.set_parameter("_opt_str", "val")
        operator.get_replica(0).output_port.set_parameter("req_int", 4321)
        operator.get_replica(0).output_port.set_parameter("_opt_int", 4321)

        for replica in operator.get_replicas():
            self.assertTrue(replica.is_equivalent_to(operator))
            for name, nested in Internals(replica).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(operator).nested_instances[name]
                ):
                    self.fail()
            for name, nested in Internals(operator).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(replica).nested_instances[name]
                ):
                    self.fail()
            self.assertIs(
                Internals(operator).parameters,
                Internals(replica).parameters,
            )
            self.assertIs(
                Internals(operator).depends_on,
                Internals(replica).depends_on,
            )
            self.assertIs(
                Internals(operator.output_port).parameters,
                Internals(replica.output_port).parameters,
            )
            self.assertIs(
                Internals(operator.output_port).depends_on,
                Internals(replica.output_port).depends_on,
            )
            self.assertIs(
                Internals(operator.input_port).parameters,
                Internals(replica.input_port).parameters,
            )
            self.assertIs(
                Internals(operator.input_port).depends_on,
                Internals(replica.input_port).depends_on,
            )
            self.assertIs(operator.output_port.req_str, replica.output_port.req_str)
            self.assertIs(operator.output_port._opt_str, replica.output_port._opt_str)
            self.assertIs(operator.output_port.req_int, replica.output_port.req_int)
            self.assertIs(operator.output_port._opt_int, replica.output_port._opt_int)

    def test_operator_replication_replica_equality_with_direct_parameter_update_on_origin(
        self,
    ):
        operator = TestOperatorWithPortPlugins("operator", 1)
        operator.set_parameter("replicationFactor", 1)

        operator.set_parameter("param", "value")
        operator.output_port.depends_on(operator.input_port)
        operator.input_port.set_parameter("param", "value")
        operator.output_port.req_str = "val"
        operator.output_port._opt_str = "val"
        operator.output_port.req_int = 4321
        operator.output_port._opt_int = 4321

        for replica in operator.get_replicas():
            self.assertTrue(replica.is_equivalent_to(operator))
            for name, nested in Internals(replica).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(operator).nested_instances[name]
                ):
                    self.fail()
            for name, nested in Internals(operator).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(replica).nested_instances[name]
                ):
                    self.fail()
            self.assertIs(
                Internals(operator).parameters,
                Internals(replica).parameters,
            )
            self.assertIs(
                Internals(operator).depends_on,
                Internals(replica).depends_on,
            )
            self.assertIs(
                Internals(operator.output_port).parameters,
                Internals(replica.output_port).parameters,
            )
            self.assertIs(
                Internals(operator.output_port).depends_on,
                Internals(replica.output_port).depends_on,
            )
            self.assertIs(
                Internals(operator.input_port).parameters,
                Internals(replica.input_port).parameters,
            )
            self.assertIs(
                Internals(operator.input_port).depends_on,
                Internals(replica.input_port).depends_on,
            )
            self.assertIs(operator.output_port.req_str, replica.output_port.req_str)
            self.assertIs(operator.output_port._opt_str, replica.output_port._opt_str)
            self.assertIs(operator.output_port.req_int, replica.output_port.req_int)
            self.assertIs(operator.output_port._opt_int, replica.output_port._opt_int)

    def test_operator_replication_replica_equality_with_direct_parameter_update_on_replica(
        self,
    ):
        operator = TestOperatorWithPortPlugins("operator")
        operator.set_parameter("replicationFactor", 1)

        operator.get_replica(0).set_parameter("param", "value")
        operator.get_replica(0).output_port.depends_on(
            operator.get_replica(0).input_port
        )
        operator.get_replica(0).input_port.set_parameter("param", "value")
        operator.get_replica(0).output_port.req_str = "val"
        operator.get_replica(0).output_port._opt_str = "val"
        operator.get_replica(0).output_port.req_int = 4321
        operator.get_replica(0).output_port._opt_int = 4321

        for replica in operator.get_replicas():
            self.assertTrue(replica.is_equivalent_to(operator))
            for name, nested in Internals(replica).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(operator).nested_instances[name]
                ):
                    self.fail()
            for name, nested in Internals(operator).nested_instances.items():
                if not nested.is_equivalent_to(
                    Internals(replica).nested_instances[name]
                ):
                    self.fail()

            self.assertIs(
                Internals(operator).parameters,
                Internals(replica).parameters,
            )
            self.assertIs(
                Internals(operator).depends_on,
                Internals(replica).depends_on,
            )
            self.assertIs(
                Internals(operator.output_port).parameters,
                Internals(replica.output_port).parameters,
            )
            self.assertIs(
                Internals(operator.output_port).depends_on,
                Internals(replica.output_port).depends_on,
            )
            self.assertIs(
                Internals(operator.input_port).parameters,
                Internals(replica.input_port).parameters,
            )
            self.assertIs(
                Internals(operator.input_port).depends_on,
                Internals(replica.input_port).depends_on,
            )
            self.assertIs(operator.output_port.req_str, replica.output_port.req_str)
            self.assertIs(operator.output_port._opt_str, replica.output_port._opt_str)
            self.assertIs(operator.output_port.req_int, replica.output_port.req_int)
            self.assertIs(operator.output_port._opt_int, replica.output_port._opt_int)

    def test_operator_replication_with_negative_factor_expect_error(self):
        with self.assertRaises(ValueError):
            TestOperatorWithPortPlugins("operator").set_parameter(
                "replicationFactor", -1
            )

    def test_logger_invocation_with_recursive_context_expect_error(self):
        operator = OperatorWithWrongLoggerPlugin("operator")

        with self.assertRaises(RecursionError):
            operator.get_logger().info("This would introduce an endless recursion")

        with self.assertRaises(RecursionError):
            operator.get_logger().debug("This would introduce an endless recursion")

        with self.assertRaises(RecursionError):
            operator.get_logger().warning("This would introduce an endless recursion")

        with self.assertRaises(RecursionError):
            operator.get_logger().error("This would introduce an endless recursion")

    def test_get_dto_with_replicas_expect_ignored_replicas_in_dto(self):
        pipeline = TestPipelineWithOperator("pipeline")

        dto = pipeline.get_dto()

        self.assertEqual(2, len(dto.spec.nestedInstances))

        for nested_instance in dto.spec.nestedInstances:
            self.assertTrue(
                ("operator_a" == nested_instance.name)
                or ("operator_b" == nested_instance.name)
            )

    def test_replica_name_retrieval(self):
        pipeline = TestPipelineWithOperator("pipeline")

        self.assertEqual("pipeline", pipeline.get_full_name())
        self.assertEqual("pipeline.operator_a", pipeline.operator_a.get_full_name())
        self.assertEqual("pipeline.operator_a_0", pipeline.operator_a_0.get_full_name())
        self.assertEqual(
            "pipeline.operator_a.input_port",
            pipeline.operator_a.input_port.get_full_name(),
        )
        self.assertEqual(
            "pipeline.operator_a_0.input_port",
            pipeline.operator_a_0.input_port.get_full_name(),
        )

    def test_replica_group_information(self):
        pipeline = TestPipelineWithOperator("pipeline")

        self.assertEqual("pipeline.operator_a", pipeline.operator_a.get_group_name())
        self.assertIs(pipeline.operator_a, pipeline.operator_a.get_group_principal())
        self.assertTrue(pipeline.operator_a.is_principal())
        self.assertEqual(0, pipeline.operator_a.get_group_index())

        self.assertEqual(
            "pipeline.operator_a.input_port",
            pipeline.operator_a.input_port.get_group_name(),
        )
        self.assertIs(
            pipeline.operator_a.input_port,
            pipeline.operator_a.input_port.get_group_principal(),
        )
        self.assertTrue(pipeline.operator_a.input_port.is_principal())
        self.assertEqual(0, pipeline.operator_a.input_port.get_group_index())

        group_index = 0
        for replica in pipeline.operator_a.get_replicas():
            self.assertEqual("pipeline.operator_a", replica.get_group_name())
            self.assertIs(pipeline.operator_a, replica.get_group_principal())
            self.assertFalse(replica.is_principal())
            self.assertEqual(group_index + 1, replica.get_group_index())

            self.assertEqual(
                "pipeline.operator_a.input_port", replica.input_port.get_group_name()
            )
            self.assertIs(
                pipeline.operator_a.input_port, replica.input_port.get_group_principal()
            )
            self.assertFalse(replica.input_port.is_principal())
            self.assertEqual(group_index + 1, replica.input_port.get_group_index())

            group_index += 1

    def test_replica_equality_and_identity(self):
        pipeline = TestPipelineWithOperator("pipeline")

        for replica in pipeline.operator_a.get_replicas():
            self.assertIsNot(pipeline.operator_a, replica)
            self.assertNotEqual(hash(pipeline.operator_a), hash(replica))

            # Both direction must be tested as in one case the Instance's
            # in the other case the ReplicaContext's __eq__ will be called.
            self.assertNotEqual(pipeline.operator_a, replica)
            self.assertNotEqual(replica, pipeline.operator_a)
            self.assertTrue(pipeline.operator_a.is_equivalent_to(replica))
            self.assertTrue(replica.is_equivalent_to(pipeline.operator_a))

        self.assertNotEqual(pipeline.operator_a_0, pipeline.operator_a_1)
        self.assertEqual(pipeline.operator_a_0, pipeline.operator_a_0)
        self.assertIs(pipeline.operator_a_0, pipeline.operator_a_0)

        for replica in pipeline.operator_a.get_replicas():
            self.assertIsNot(pipeline.operator_a.input_port, replica.input_port)
            self.assertNotEqual(
                hash(pipeline.operator_a.input_port), hash(replica.input_port)
            )

            # Both direction must be tested as in one case the Instance's
            # in the other case the ReplicaContext's __eq__ will be called.
            self.assertNotEqual(pipeline.operator_a.input_port, replica.input_port)
            self.assertNotEqual(replica.input_port, pipeline.operator_a.input_port)
            self.assertTrue(
                pipeline.operator_a.input_port.is_equivalent_to(replica.input_port)
            )
            self.assertTrue(
                replica.input_port.is_equivalent_to(pipeline.operator_a.input_port)
            )

        self.assertNotEqual(
            pipeline.operator_a_0.input_port, pipeline.operator_a_1.input_port
        )
        self.assertEqual(
            pipeline.operator_a_0.input_port, pipeline.operator_a_0.input_port
        )
        self.assertIs(
            pipeline.operator_a_0.input_port, pipeline.operator_a_0.input_port
        )

    def test_replica_dto_creation(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_a_dto = pipeline.operator_a.get_dto()
        operator_a_0_dto = pipeline.operator_a_0.get_dto()

        self.assertEqual("operator_a", operator_a_dto.name)
        self.assertEqual("operator_a_0", operator_a_0_dto.name)

    def test_replica_nested_instance_wrapping(self):
        """
        It is to be ensured that the Instance object in the nested instance list are
        wrapped into ReplicaContext, if accessed through the replica context.
        """
        pipeline = TestPipelineWithOperator("pipeline")

        for plugin in Internals(pipeline.operator_a_0).nested_instances.values():
            print(isinstance(plugin, ReplicaContext))
            print(plugin.get_full_name())
            self.assertIn(
                plugin.get_full_name(),
                {
                    "pipeline.operator_a_0.input_port",
                    "pipeline.operator_a_0.output_port",
                },
            )


# channel
# tényleg lemennek-e executálás közben a replicált dolgok
# docs
