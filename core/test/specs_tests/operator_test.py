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
from pypz.core.specs.instance import InstanceGroup
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.core.specs.utils import Internals

from core.test.specs_tests.operator_test_resources import (
    EmptyReplicaTestPipeline,
    OperatorWithWrongLoggerPlugin,
    ReplicaTestOperatorWithSimplePluginWithCustomName,
    ReplicaTestPipeline,
    ReplicaTestPlugin,
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
            replica = pipeline.operator_a.get_replica(replica_idx).materialize()

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
            replica = pipeline.operator_a.get_replica(replica_idx).materialize()

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertEqual(
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
            replica = pipeline.operator_a.get_replica(replica_idx).materialize()

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertEqual(
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
            replica = pipeline.operator_a.get_replica(replica_idx).materialize()

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertEqual(
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
            replica = pipeline.operator_a.get_replica(replica_idx).materialize()

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertEqual(
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
            replica = pipeline.operator_a.get_replica(replica_idx).materialize()

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertEqual(
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
            replica = pipeline.operator_a.get_replica(replica_idx).materialize()

            self.assertEqual(replica.param_a, pipeline.operator_a.param_a)
            self.assertEqual(replica.param_b, pipeline.operator_a.param_b)
            self.assertEqual(replica.param_c, pipeline.operator_a.param_c)
            self.assertEqual(replica.param_d, pipeline.operator_a.param_d)
            self.assertEqual(replica.param_e, pipeline.operator_a.param_e)

            self.assertEqual(
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
            replica = operator.get_replica(replica_idx).materialize()

            self.assertEqual(operator, replica.get_group_principal())
            self.assertEqual(operator.get_simple_name(), replica.get_group_name())
            self.assertFalse(replica.is_principal())
            self.assertEqual(f"operator_{replica_idx}", replica.get_full_name())

    def test_operator_replication_replica_equality_without_updates(self):
        operator = TestOperatorWithPortPlugins("operator", 5)
        operator_internals = Internals(operator)

        for replica in operator.get_replicas():
            replica_internals = Internals(replica.materialize())

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
        operator_internals = Internals(operator)

        operator.set_parameter("param", "value")
        operator.output_port.depends_on(operator.input_port)
        operator.input_port.set_parameter("param", "value")
        operator.output_port.set_parameter("req_str", "val")
        operator.output_port.set_parameter("_opt_str", "val")
        operator.output_port.set_parameter("req_int", 4321)
        operator.output_port.set_parameter("_opt_int", 4321)

        for replica in operator.get_replicas():
            replica_internals = Internals(replica.materialize())

            self.assertEqual(
                operator_internals.nested_instances,
                replica_internals.nested_instances,
            )
            self.assertEqual(
                id(operator_internals.parameters),
                id(replica_internals.parameters),
            )
            self.assertEqual(
                id(operator_internals.depends_on),
                id(replica_internals.depends_on),
            )
            self.assertEqual(
                id(Internals(operator.output_port).parameters),
                id(Internals(replica.output_port).parameters),
            )
            self.assertEqual(
                id(Internals(operator.output_port).depends_on),
                id(Internals(replica.output_port).depends_on),
            )
            self.assertEqual(
                id(Internals(operator.input_port).parameters),
                id(Internals(replica.input_port).parameters),
            )
            self.assertEqual(
                id(Internals(operator.input_port).depends_on),
                id(Internals(replica.input_port).depends_on),
            )
            self.assertEqual(
                id(operator.output_port.req_str), id(replica.output_port.req_str)
            )
            self.assertEqual(
                id(operator.output_port._opt_str), id(replica.output_port._opt_str)
            )
            self.assertEqual(
                id(operator.output_port.req_int), id(replica.output_port.req_int)
            )
            self.assertEqual(
                id(operator.output_port._opt_int), id(replica.output_port._opt_int)
            )

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
            materialized_replica = replica.materialize()
            self.assertTrue(operator.is_equivalent_to(materialized_replica))

    def test_operator_replication_replica_equality_with_direct_parameter_update_on_origin(
        self,
    ):
        operator = TestOperatorWithPortPlugins("operator", 1)

        operator.set_parameter("param", "value")
        operator.output_port.depends_on(operator.input_port)
        operator.input_port.set_parameter("param", "value")
        operator.output_port.req_str = "val"
        operator.output_port._opt_str = "val"
        operator.output_port.req_int = 4321
        operator.output_port._opt_int = 4321

        for replica in operator.get_replicas():
            materialized_replica = replica.materialize()
            self.assertEqual(
                Internals(operator).nested_instances,
                Internals(materialized_replica).nested_instances,
            )
            self.assertEqual(
                id(Internals(operator).parameters),
                id(Internals(materialized_replica).parameters),
            )
            self.assertEqual(
                id(Internals(operator).depends_on),
                id(Internals(materialized_replica).depends_on),
            )
            self.assertEqual(
                id(Internals(operator.output_port).parameters),
                id(Internals(materialized_replica.output_port).parameters),
            )
            self.assertEqual(
                id(Internals(operator.output_port).depends_on),
                id(Internals(materialized_replica.output_port).depends_on),
            )
            self.assertEqual(
                id(Internals(operator.input_port).parameters),
                id(Internals(materialized_replica.input_port).parameters),
            )
            self.assertEqual(
                id(Internals(operator.input_port).depends_on),
                id(Internals(materialized_replica.input_port).depends_on),
            )
            self.assertEqual(
                id(operator.output_port.req_str),
                id(materialized_replica.output_port.req_str),
            )
            self.assertEqual(
                id(operator.output_port._opt_str),
                id(materialized_replica.output_port._opt_str),
            )
            self.assertEqual(
                id(operator.output_port.req_int),
                id(materialized_replica.output_port.req_int),
            )
            self.assertEqual(
                id(operator.output_port._opt_int),
                id(materialized_replica.output_port._opt_int),
            )

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
            materialized_replica = replica.materialize()
            self.assertTrue(operator.is_equivalent_to(materialized_replica))

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

    def test_replication_replica_phase_basics(self):
        pipeline = TestPipelineWithOperator("pipeline")

        replica_idx = 0
        for replica in pipeline.operator_a.get_replicas():
            self.assertTrue(isinstance(replica, Operator.Replica))
            self.assertTrue(isinstance(replica, Operator))
            self.assertTrue(isinstance(replica, InstanceGroup))

            self.assertEqual(
                f"{pipeline.operator_a.get_simple_name()}_{replica_idx}",
                replica.get_simple_name(),
            )
            self.assertEqual(
                f"{pipeline.operator_a.get_full_name()}_{replica_idx}",
                replica.get_full_name(),
            )

            self.assertIs(replica.get_original(), pipeline.operator_a)
            replica_idx += 1

        # From DTO
        pipeline = Pipeline.create_from_dto(pipeline.get_dto())
        replica_idx = 0
        for replica in pipeline.operator_a.get_replicas():
            self.assertTrue(isinstance(replica, Operator.Replica))
            self.assertTrue(isinstance(replica, Operator))
            self.assertTrue(isinstance(replica, InstanceGroup))

            self.assertEqual(
                f"{pipeline.operator_a.get_simple_name()}_{replica_idx}",
                replica.get_simple_name(),
            )
            self.assertEqual(
                f"{pipeline.operator_a.get_full_name()}_{replica_idx}",
                replica.get_full_name(),
            )

            self.assertIs(replica.get_original(), pipeline.operator_a)
            replica_idx += 1

    def test_replication_replica_phase_group_information(self):
        pipeline = TestPipelineWithOperator("pipeline")

        replica_idx = 0
        for replica in pipeline.operator_a.get_replicas():
            self.assertEqual(
                pipeline.operator_a.get_full_name(), replica.get_group_name()
            )
            self.assertEqual(replica_idx + 1, replica.get_group_index())
            self.assertIs(replica.get_group_principal(), pipeline.operator_a)
            self.assertFalse(replica.is_principal())
            self.assertEqual(
                len(pipeline.operator_a.get_replicas()) + 1, replica.get_group_size()
            )
            replica_idx += 1

        # From DTO
        pipeline = Pipeline.create_from_dto(pipeline.get_dto())

        replica_idx = 0
        for replica in pipeline.operator_a.get_replicas():
            self.assertEqual(
                pipeline.operator_a.get_full_name(), replica.get_group_name()
            )
            self.assertEqual(replica_idx + 1, replica.get_group_index())
            self.assertIs(replica.get_group_principal(), pipeline.operator_a)
            self.assertFalse(replica.is_principal())
            self.assertEqual(
                len(pipeline.operator_a.get_replicas()) + 1, replica.get_group_size()
            )
            replica_idx += 1

    def test_replication_replica_phase_dto(self):
        pipeline = TestPipelineWithOperator("pipeline")

        replica_idx = 0
        for replica in pipeline.operator_a.get_replicas():
            dto = replica.get_dto()
            self.assertEqual(
                f"{pipeline.operator_a.get_simple_name()}_{replica_idx}", dto.name
            )
            replica_idx += 1

        # From DTO
        pipeline = Pipeline.create_from_dto(pipeline.get_dto())

        replica_idx = 0
        for replica in pipeline.operator_a.get_replicas():
            dto = replica.get_dto()
            self.assertEqual(
                f"{pipeline.operator_a.get_simple_name()}_{replica_idx}", dto.name
            )
            replica_idx += 1

    def test_replication_replica_phase_equality(self):
        pipeline = TestPipelineWithOperator("pipeline")

        for replica in pipeline.operator_a.get_replicas():
            self.assertNotEqual(pipeline.operator_a, replica)
            self.assertNotEqual(replica, pipeline.operator_a)
            self.assertEqual(replica, replica)
            self.assertTrue(pipeline.operator_a.is_equivalent_to(replica))
            self.assertTrue(replica.is_equivalent_to(pipeline.operator_a))

            self.assertNotEqual(hash(pipeline.operator_a), hash(replica))

        self.assertNotEqual(
            pipeline.operator_a.get_replica(0), pipeline.operator_a.get_replica(1)
        )
        self.assertNotEqual(
            hash(pipeline.operator_a.get_replica(0)),
            hash(pipeline.operator_a.get_replica(1)),
        )
        self.assertTrue(
            pipeline.operator_a.get_replica(0).is_equivalent_to(
                pipeline.operator_a.get_replica(1)
            )
        )

        # From DTO
        pipeline = Pipeline.create_from_dto(pipeline.get_dto())

        for replica in pipeline.operator_a.get_replicas():
            self.assertNotEqual(pipeline.operator_a, replica)
            self.assertNotEqual(replica, pipeline.operator_a)
            self.assertEqual(replica, replica)
            self.assertTrue(pipeline.operator_a.is_equivalent_to(replica))
            self.assertTrue(replica.is_equivalent_to(pipeline.operator_a))

            self.assertNotEqual(hash(pipeline.operator_a), hash(replica))

        self.assertNotEqual(
            pipeline.operator_a.get_replica(0), pipeline.operator_a.get_replica(1)
        )
        self.assertNotEqual(
            hash(pipeline.operator_a.get_replica(0)),
            hash(pipeline.operator_a.get_replica(1)),
        )
        self.assertTrue(
            pipeline.operator_a.get_replica(0).is_equivalent_to(
                pipeline.operator_a.get_replica(1)
            )
        )

    def test_replication_materialized_phase_basics(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_a_0 = pipeline.operator_a.get_replica(0)
        mat1 = operator_a_0.materialize()
        mat2 = operator_a_0.materialize()

        self.assertIsNot(mat1, mat2)

        replica_idx = 0
        for raw_replica in pipeline.operator_a.get_replicas():
            replica = raw_replica.materialize()
            self.assertFalse(isinstance(replica, Operator.Replica))
            self.assertTrue(isinstance(replica, Operator))
            self.assertTrue(isinstance(replica, InstanceGroup))

            self.assertNotEqual(hash(pipeline.operator_a), hash(replica))
            self.assertEqual(0, len(replica.get_replicas()))

            self.assertEqual(
                f"{pipeline.operator_a.get_simple_name()}_{replica_idx}",
                replica.get_simple_name(),
            )
            self.assertEqual(
                f"{pipeline.operator_a.get_full_name()}_{replica_idx}",
                replica.get_full_name(),
            )

            self.assertEqual(
                f"{pipeline.operator_a.get_full_name()}_{replica_idx}."
                f"{pipeline.operator_a.input_port.get_simple_name()}",
                replica.input_port.get_full_name(),
            )
            self.assertEqual(
                f"{pipeline.operator_a.get_full_name()}_{replica_idx}."
                f"{pipeline.operator_a.output_port.get_simple_name()}",
                replica.output_port.get_full_name(),
            )

            self.assertIs(replica.get_context(), pipeline)
            replica_idx += 1

    def test_replication_materialized_phase_group_information(self):
        pipeline = TestPipelineWithOperator("pipeline")

        replica_idx = 0
        for raw_replica in pipeline.operator_a.get_replicas():
            replica = raw_replica.materialize()

            self.assertEqual(
                pipeline.operator_a.get_full_name(), replica.get_group_name()
            )
            self.assertEqual(replica_idx + 1, replica.get_group_index())
            self.assertIs(replica.get_group_principal(), pipeline.operator_a)
            self.assertFalse(replica.is_principal())
            self.assertEqual(
                len(pipeline.operator_a.get_replicas()) + 1, replica.get_group_size()
            )
            replica_idx += 1

    def test_replication_materialized_phase_dto(self):
        pipeline = TestPipelineWithOperator("pipeline")

        replica_idx = 0
        for raw_replica in pipeline.operator_a.get_replicas():
            replica = raw_replica.materialize()

            dto = replica.get_dto()
            self.assertEqual(
                f"{pipeline.operator_a.get_simple_name()}_{replica_idx}", dto.name
            )
            replica_idx += 1

    def test_replication_materialized_phase_equality(self):
        pipeline = TestPipelineWithOperator("pipeline")
        pipeline.operator_a.set_parameter("param_a", 10)
        pipeline.operator_a.set_parameter("param_b", 11)
        pipeline.operator_a.set_parameter("param_c", 12)
        pipeline.operator_a.set_parameter("param_d", 13)
        pipeline.operator_a.set_parameter("param_e", 14)
        pipeline.operator_a.set_parameter("dynamic", "value")

        pipeline.operator_a.set_parameter("param_increment", 2)
        self.assertEqual(2, pipeline.operator_a.increment_value)
        pipeline.operator_a.set_parameter("param_increment", 3)
        self.assertEqual(5, pipeline.operator_a.increment_value)

        operator_a_internals = Internals(pipeline.operator_a)
        for raw_replica in pipeline.operator_a.get_replicas():
            replica = raw_replica.materialize()
            replica_internals = Internals(replica)

            self.assertIsNot(
                operator_a_internals.parameters, replica_internals.parameters
            )
            self.assertEqual(
                operator_a_internals.parameters, replica_internals.parameters
            )

            self.assertEqual(5, replica.get_parameter("replicationFactor"))
            self.assertEqual("test_image", replica.get_parameter("operatorImageName"))
            self.assertEqual(5, replica._replication_factor)
            self.assertEqual("test_image", replica._operator_image_name)
            self.assertEqual(10, replica.param_a)
            self.assertEqual(11, replica.param_b)
            self.assertEqual(12, replica.param_c)
            self.assertEqual(13, replica.param_d)
            self.assertEqual(14, replica.param_e)
            self.assertEqual("value", replica.get_parameter("dynamic"))
            self.assertEqual(3, replica.get_parameter("param_increment"))
            self.assertEqual(3, replica.param_increment)
            self.assertEqual(5, replica.increment_value)

            replica.set_parameter("param_a", 20)

            self.assertNotEqual(pipeline.operator_a.param_a, replica.param_a)

            # Test parameter independence
            replica.set_parameter("replicationFactor", 0)
            replica.set_parameter("operatorImageName", "other_image")
            self.assertNotEqual(
                pipeline.operator_a._replication_factor, replica._replication_factor
            )
            self.assertNotEqual(
                pipeline.operator_a._operator_image_name, replica._operator_image_name
            )

            # Test, if parameter update callback are bound correctly
            replica.set_parameter("param_all", 33)
            self.assertEqual(10, pipeline.operator_a.param_a)
            self.assertEqual(11, pipeline.operator_a.param_b)
            self.assertEqual(12, pipeline.operator_a.param_c)
            self.assertEqual(13, pipeline.operator_a.param_d)
            self.assertEqual(14, pipeline.operator_a.param_e)
            self.assertEqual(33, replica.param_a)
            self.assertEqual(33, replica.param_b)
            self.assertEqual(33, replica.param_c)
            self.assertEqual(33, replica.param_d)
            self.assertEqual(33, replica.param_e)

            self.assertEqual(
                operator_a_internals.depends_on, replica_internals.depends_on
            )
            self.assertIsNot(
                operator_a_internals.depends_on, replica_internals.depends_on
            )

            self.assertIsNot(pipeline.operator_a.input_port, replica.input_port)
            self.assertNotEqual(pipeline.operator_a.input_port, replica.input_port)
            self.assertTrue(
                pipeline.operator_a.input_port.is_equivalent_to(replica.input_port)
            )

            self.assertIsNot(pipeline.operator_a.output_port, replica.output_port)
            self.assertNotEqual(pipeline.operator_a.output_port, replica.output_port)
            self.assertTrue(
                pipeline.operator_a.output_port.is_equivalent_to(replica.output_port)
            )

    def test_replication_materialized_phase_identity(self):
        pipeline = ReplicaTestPipeline("pipeline")

        # Pre-materialization checks
        # The proxy practically returns the original so identity shall be given
        replica_0 = pipeline.replicated.get_replica(0)

        self.assertIs(pipeline.replicated, replica_0.this)
        self.assertIs(pipeline.replicated.plugin, replica_0.plugin)
        self.assertIs(pipeline.replicated.plugin, replica_0.plugin.this)
        self.assertIs(pipeline.operator, next(iter(replica_0._Instance__depends_on)))
        self.assertIs(
            pipeline.operator.output_port,
            next(iter(replica_0.input_port._PortPlugin__connected_ports)),
        )

        # Post-materialization checks
        # Upon creation of the replica based on the provided class, the entire
        # internals will be set up, so identity of replica internals shall not be
        # the identity of original internals. However, if there is a reference
        # to an external entity like through connection or dependency, then
        # those shall be shared and therefore the identities shall be the same.
        materialized = replica_0.materialize()

        self.assertIsNot(pipeline.replicated, materialized)
        self.assertIsNot(pipeline.replicated, materialized.this)
        self.assertIs(materialized, materialized.this)
        self.assertIs(pipeline.replicated.ctx, materialized.ctx)

        self.assertIs(materialized.plugin, materialized.plugin.this)
        self.assertIsNot(pipeline.replicated.plugin, materialized.plugin)
        self.assertIsNot(pipeline.replicated.plugin, materialized.plugin.this)
        self.assertIsNot(pipeline.replicated.plugin.this, materialized.plugin.this)

        self.assertIs(materialized, materialized.plugin.ctx)
        self.assertIsNot(pipeline.replicated, materialized.plugin.ctx)
        self.assertIsNot(pipeline.replicated.plugin.ctx, materialized.plugin.ctx)

        self.assertIs(pipeline.operator, next(iter(materialized._Instance__depends_on)))
        self.assertIs(
            pipeline.operator.output_port,
            next(iter(materialized.input_port._PortPlugin__connected_ports)),
        )

        self.assertIs(pipeline.replicated.sibling, materialized.sibling)
        self.assertIs(pipeline.operator, materialized.sibling)
        # sibling is treated as nested instance, hence will be recreated
        self.assertIsNot(
            pipeline.replicated.plugin.sibling, materialized.plugin.sibling
        )
        self.assertIs(materialized.input_port, materialized.plugin.sibling)
        self.assertIs(
            pipeline.replicated.plugin.operator_sibling,
            materialized.plugin.operator_sibling,
        )
        self.assertIs(pipeline.operator.plugin, materialized.plugin.operator_sibling)

    def test_replication_materialized_phase_with_operator_from_string(self):
        pipeline_string = """
        dependsOn: []
        name: pipeline
        parameters: {}
        spec:
          expectedParameters: {}
          location: null
          name: core.test.specs_tests.operator_test_resources:EmptyReplicaTestPipeline
          nestedInstanceType: <class 'pypz.core.specs.operator.Operator'>
          nestedInstances:
          - connections: []
            dependsOn: []
            name: operator
            parameters:
              operatorImageName: null
              replicationFactor: 1
            spec:
              expectedParameters:
                operatorImageName:
                  currentValue: null
                  description: The image containing the operator's resources. It will be used
                    mainly by the deployers.
                  required: false
                  type: str
                replicationFactor:
                  currentValue: 0
                  description: Determines, how many replicas shall be created from the original.
                  required: false
                  type: int
              location: null
              name: core.test.specs_tests.operator_test_resources:ReplicaTestOperatorWithSimplePlugin
              nestedInstanceType: <class 'pypz.core.specs.plugin.Plugin'>
              nestedInstances:
              - dependsOn:
                - plugin1
                name: plugin2
                parameters: {}
                spec:
                  expectedParameters: {}
                  location: null
                  name: core.test.specs_tests.operator_test_resources:ReplicaTestPlugin
                  nestedInstanceType: null
                  nestedInstances: null
                  types:
                  - <class 'pypz.core.specs.plugin.Plugin'>
              types:
              - <class 'pypz.core.specs.operator.Operator'>

          types:
          - <class 'pypz.core.specs.operator.Operator'>
        """

        pipeline = Pipeline.create_from_string(pipeline_string)
        replica = pipeline.operator_0.materialize()
        self.assertTrue(hasattr(replica, "plugin2"))
        self.assertTrue(isinstance(replica.plugin2, ReplicaTestPlugin))
        self.assertEqual("pipeline.operator_0.plugin2", replica.plugin2.get_full_name())
        self.assertIsNot(pipeline.operator.plugin2, replica.plugin2)
        self.assertNotEqual(pipeline.operator.plugin2, replica.plugin2)
        self.assertTrue(pipeline.operator.plugin2.is_equivalent_to(replica.plugin2))
        self.assertIsNot(
            pipeline.operator.plugin1, next(iter(replica.plugin2._Instance__depends_on))
        )
        self.assertIs(
            replica.plugin1, next(iter(replica.plugin2._Instance__depends_on))
        )

    def test_replication_materialized_phase_with_plugin_added_from_outside(self):
        pipeline = EmptyReplicaTestPipeline("pipeline")
        pipeline.operator = ReplicaTestOperatorWithSimplePluginWithCustomName(
            "operator", context=pipeline
        )
        pipeline.operator.plugin2 = ReplicaTestPlugin(
            "plugin22", context=pipeline.operator
        )
        pipeline.operator.set_parameter("replicationFactor", 1)
        replica = pipeline.operator_0.materialize()

        self.assertEqual(2, len(Internals(pipeline.operator).nested_instances))
        self.assertTrue(hasattr(pipeline.operator, "plugin1"))
        self.assertTrue(isinstance(pipeline.operator.plugin1, ReplicaTestPlugin))
        self.assertEqual(
            "pipeline.operator.plugin", pipeline.operator.plugin1.get_full_name()
        )

        self.assertTrue(hasattr(pipeline.operator, "plugin2"))
        self.assertTrue(isinstance(pipeline.operator.plugin2, ReplicaTestPlugin))
        self.assertEqual(
            "pipeline.operator.plugin22", pipeline.operator.plugin2.get_full_name()
        )

        self.assertEqual(2, len(Internals(replica).nested_instances))
        self.assertTrue(hasattr(replica, "plugin1"))
        self.assertTrue(isinstance(replica.plugin1, ReplicaTestPlugin))
        self.assertEqual("pipeline.operator_0.plugin", replica.plugin1.get_full_name())

        self.assertTrue(hasattr(replica, "plugin2"))
        self.assertTrue(isinstance(replica.plugin2, ReplicaTestPlugin))
        self.assertEqual(
            "pipeline.operator_0.plugin22", replica.plugin2.get_full_name()
        )
