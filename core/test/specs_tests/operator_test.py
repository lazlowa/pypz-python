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
from pypz.core.specs.dtos import OperatorInstanceDTO, OperatorConnection, OperatorConnectionSource
from core.test.specs_tests.operator_test_resources import TestPipelineWithOperator, TestOperatorWithPortPlugins, \
    OperatorWithWrongLoggerPlugin


class OperatorInstanceTest(unittest.TestCase):

    def test_operator_dto_generation_with_connections_and_replication(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_a_dto = pipeline.operator_a.get_dto()

        self.assertEqual(5, operator_a_dto.parameters["replicationFactor"])
        self.assertEqual([OperatorConnection("input_port", OperatorConnectionSource("operator_b", "output_port"))],
                         operator_a_dto.connections)

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
            connections=[OperatorConnection("input_port", OperatorConnectionSource("operator_a", "output_port"))])

        pipeline.operator_b.update(operator_dto)

        self.assertEqual(1, len(pipeline.operator_b.input_port.get_connected_ports()))
        self.assertTrue(pipeline.operator_a.output_port in pipeline.operator_b.input_port.get_connected_ports())

    def test_operator_update_multiple_with_valid_connections_expect_no_change(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[OperatorConnection("input_port", OperatorConnectionSource("operator_a", "output_port"))])

        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)
        pipeline.operator_b.update(operator_dto)

        self.assertEqual(1, len(pipeline.operator_b.input_port.get_connected_ports()))
        self.assertTrue(pipeline.operator_a.output_port in pipeline.operator_b.input_port.get_connected_ports())

    def test_operator_update_with_non_existing_input_port_expect_error(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[OperatorConnection("non_existing", OperatorConnectionSource("operator_b", "output_port"))])

        with self.assertRaises(AttributeError):
            pipeline.operator_a.update(operator_dto)

    def test_operator_update_with_non_existing_source_instance_expect_error(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[OperatorConnection("input_port", OperatorConnectionSource("non_existing", "output_port"))])

        with self.assertRaises(AttributeError):
            pipeline.operator_a.update(operator_dto)

    def test_operator_update_with_non_existing_output_port_expect_error(self):
        pipeline = TestPipelineWithOperator("pipeline")

        operator_dto = OperatorInstanceDTO(
            connections=[OperatorConnection("input_port", OperatorConnectionSource("operator_b", "non_existing"))])

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
        self.assertEqual(pipeline.operator_a.get_full_name(), pipeline.operator_a.get_group_name())
        self.assertTrue(pipeline.operator_a.is_principal())
        self.assertEqual("pipeline.operator_a", pipeline.operator_a.get_full_name())

        self.assertEqual(6, pipeline.operator_a.output_port.get_group_size())
        self.assertEqual(0, pipeline.operator_a.output_port.get_group_index())
        self.assertEqual(pipeline.operator_a.output_port, pipeline.operator_a.output_port.get_group_principal())
        self.assertEqual(pipeline.operator_a.output_port.get_full_name(),
                         pipeline.operator_a.output_port.get_group_name())
        self.assertTrue(pipeline.operator_a.output_port.is_principal())

        self.assertEqual(6, pipeline.operator_a.input_port.get_group_size())
        self.assertEqual(0, pipeline.operator_a.input_port.get_group_index())
        self.assertEqual(pipeline.operator_a.input_port, pipeline.operator_a.input_port.get_group_principal())
        self.assertEqual(pipeline.operator_a.input_port.get_full_name(),
                         pipeline.operator_a.input_port.get_group_name())
        self.assertTrue(pipeline.operator_a.input_port.is_principal())

        for replica_idx in range(pipeline.operator_a.get_replication_factor()):
            replica = pipeline.operator_a.get_replica(replica_idx)

            self.assertEqual(6, replica.get_group_size())
            self.assertEqual(replica_idx+1, replica.get_group_index())
            self.assertEqual(pipeline.operator_a, replica.get_group_principal())
            self.assertEqual(pipeline.operator_a.get_full_name(), replica.get_group_name())
            self.assertFalse(replica.is_principal())
            self.assertEqual(f"pipeline.operator_a_{replica_idx}", replica.get_full_name())

            self.assertEqual(6, replica.output_port.get_group_size())
            self.assertEqual(replica_idx+1, replica.output_port.get_group_index())
            self.assertEqual(pipeline.operator_a.output_port, replica.output_port.get_group_principal())
            self.assertEqual(pipeline.operator_a.output_port.get_full_name(), replica.output_port.get_group_name())
            self.assertFalse(replica.output_port.is_principal())
            self.assertEqual(f"pipeline.operator_a_{replica_idx}.output_port", replica.output_port.get_full_name())

            self.assertEqual(6, replica.input_port.get_group_size())
            self.assertEqual(replica_idx+1, replica.input_port.get_group_index())
            self.assertEqual(pipeline.operator_a.input_port, replica.input_port.get_group_principal())
            self.assertEqual(pipeline.operator_a.input_port.get_full_name(), replica.input_port.get_group_name())
            self.assertFalse(replica.input_port.is_principal())
            self.assertEqual(f"pipeline.operator_a_{replica_idx}.input_port", replica.input_port.get_full_name())

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

            self.assertIs(pipeline.operator_a.get_protected().get_parameters(),
                          replica.get_protected().get_parameters())

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

            self.assertIs(pipeline.operator_a.get_protected().get_parameters(),
                          replica.get_protected().get_parameters())

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

            self.assertIs(pipeline.operator_a.get_protected().get_parameters(),
                          replica.get_protected().get_parameters())

    def test_operator_replication_indirect_set_parameter_attribute_expect_equality(self):
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

            self.assertIs(pipeline.operator_a.get_protected().get_parameters(),
                          replica.get_protected().get_parameters())

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

            self.assertIs(pipeline.operator_a.get_protected().get_parameters(),
                          replica.get_protected().get_parameters())

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

            self.assertIs(pipeline.operator_a.get_protected().get_parameters(),
                          replica.get_protected().get_parameters())

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

        for replica in operator.get_replicas():
            self.assertEqual(5, replica.get_replication_factor())
            self.assertEqual(id(operator), id(replica.get_group_principal()))
            self.assertEqual(0, len(replica.get_replicas()))
            self.assertEqual(id(operator.get_context()), id(replica.get_context()))
            self.assertEqual(operator.get_protected().get_nested_instances(),
                             replica.get_protected().get_nested_instances())
            self.assertEqual(operator.get_protected().get_parameters(), replica.get_protected().get_parameters())
            self.assertEqual(operator.get_protected().get_depends_on(), replica.get_protected().get_depends_on())

    def test_operator_replication_replica_equality_with_updates_on_origin(self):
        operator = TestOperatorWithPortPlugins("operator", 1)

        operator.set_parameter("param", "value")
        operator.output_port.depends_on(operator.input_port)
        operator.input_port.set_parameter("param", "value")
        operator.output_port.set_parameter("req_str", "val")
        operator.output_port.set_parameter("_opt_str", "val")
        operator.output_port.set_parameter("req_int", 4321)
        operator.output_port.set_parameter("_opt_int", 4321)

        for replica in operator.get_replicas():
            self.assertEqual(operator.get_protected().get_nested_instances(),
                             replica.get_protected().get_nested_instances())
            self.assertEqual(id(operator.get_protected().get_parameters()),
                             id(replica.get_protected().get_parameters()))
            self.assertEqual(id(operator.get_protected().get_depends_on()),
                             id(replica.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.get_protected().get_parameters()),
                             id(replica.output_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.output_port.get_protected().get_depends_on()),
                             id(replica.output_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.input_port.get_protected().get_parameters()),
                             id(replica.input_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.input_port.get_protected().get_depends_on()),
                             id(replica.input_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.req_str), id(replica.output_port.req_str))
            self.assertEqual(id(operator.output_port._opt_str), id(replica.output_port._opt_str))
            self.assertEqual(id(operator.output_port.req_int), id(replica.output_port.req_int))
            self.assertEqual(id(operator.output_port._opt_int), id(replica.output_port._opt_int))

    def test_operator_replication_replica_equality_with_updates_on_replica(self):
        operator = TestOperatorWithPortPlugins("operator")
        operator.set_parameter("replicationFactor", 1)

        operator.get_replica(0).set_parameter("param", "value")
        operator.get_replica(0).output_port.depends_on(operator.get_replica(0).input_port)
        operator.get_replica(0).input_port.set_parameter("param", "value")
        operator.get_replica(0).output_port.set_parameter("req_str", "val")
        operator.get_replica(0).output_port.set_parameter("_opt_str", "val")
        operator.get_replica(0).output_port.set_parameter("req_int", 4321)
        operator.get_replica(0).output_port.set_parameter("_opt_int", 4321)

        for replica in operator.get_replicas():
            self.assertEqual(operator.get_protected().get_nested_instances(),
                             replica.get_protected().get_nested_instances())
            self.assertEqual(id(operator.get_protected().get_parameters()),
                             id(replica.get_protected().get_parameters()))
            self.assertEqual(id(operator.get_protected().get_depends_on()),
                             id(replica.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.get_protected().get_parameters()),
                             id(replica.output_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.output_port.get_protected().get_depends_on()),
                             id(replica.output_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.input_port.get_protected().get_parameters()),
                             id(replica.input_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.input_port.get_protected().get_depends_on()),
                             id(replica.input_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.req_str), id(replica.output_port.req_str))
            self.assertEqual(id(operator.output_port._opt_str), id(replica.output_port._opt_str))
            self.assertEqual(id(operator.output_port.req_int), id(replica.output_port.req_int))
            self.assertEqual(id(operator.output_port._opt_int), id(replica.output_port._opt_int))

    def test_operator_replication_replica_equality_with_direct_parameter_update_on_origin(self):
        operator = TestOperatorWithPortPlugins("operator", 1)

        operator.set_parameter("param", "value")
        operator.output_port.depends_on(operator.input_port)
        operator.input_port.set_parameter("param", "value")
        operator.output_port.req_str = "val"
        operator.output_port._opt_str = "val"
        operator.output_port.req_int = 4321
        operator.output_port._opt_int = 4321

        for replica in operator.get_replicas():
            self.assertEqual(operator.get_protected().get_nested_instances(),
                             replica.get_protected().get_nested_instances())
            self.assertEqual(id(operator.get_protected().get_parameters()),
                             id(replica.get_protected().get_parameters()))
            self.assertEqual(id(operator.get_protected().get_depends_on()),
                             id(replica.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.get_protected().get_parameters()),
                             id(replica.output_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.output_port.get_protected().get_depends_on()),
                             id(replica.output_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.input_port.get_protected().get_parameters()),
                             id(replica.input_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.input_port.get_protected().get_depends_on()),
                             id(replica.input_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.req_str), id(replica.output_port.req_str))
            self.assertEqual(id(operator.output_port._opt_str), id(replica.output_port._opt_str))
            self.assertEqual(id(operator.output_port.req_int), id(replica.output_port.req_int))
            self.assertEqual(id(operator.output_port._opt_int), id(replica.output_port._opt_int))

    def test_operator_replication_replica_equality_with_direct_parameter_update_on_replica(self):
        operator = TestOperatorWithPortPlugins("operator")
        operator.set_parameter("replicationFactor", 1)

        operator.get_replica(0).set_parameter("param", "value")
        operator.get_replica(0).output_port.depends_on(operator.get_replica(0).input_port)
        operator.get_replica(0).input_port.set_parameter("param", "value")
        operator.get_replica(0).output_port.req_str = "val"
        operator.get_replica(0).output_port._opt_str = "val"
        operator.get_replica(0).output_port.req_int = 4321
        operator.get_replica(0).output_port._opt_int = 4321

        for replica in operator.get_replicas():
            self.assertEqual(operator.get_protected().get_nested_instances(),
                             replica.get_protected().get_nested_instances())
            self.assertEqual(id(operator.get_protected().get_parameters()),
                             id(replica.get_protected().get_parameters()))
            self.assertEqual(id(operator.get_protected().get_depends_on()),
                             id(replica.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.get_protected().get_parameters()),
                             id(replica.output_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.output_port.get_protected().get_depends_on()),
                             id(replica.output_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.input_port.get_protected().get_parameters()),
                             id(replica.input_port.get_protected().get_parameters()))
            self.assertEqual(id(operator.input_port.get_protected().get_depends_on()),
                             id(replica.input_port.get_protected().get_depends_on()))
            self.assertEqual(id(operator.output_port.req_str), id(replica.output_port.req_str))
            self.assertEqual(id(operator.output_port._opt_str), id(replica.output_port._opt_str))
            self.assertEqual(id(operator.output_port.req_int), id(replica.output_port.req_int))
            self.assertEqual(id(operator.output_port._opt_int), id(replica.output_port._opt_int))

    def test_operator_replication_with_negative_factor_expect_error(self):
        with self.assertRaises(ValueError):
            TestOperatorWithPortPlugins("operator").set_parameter("replicationFactor", -1)

    def test_logger_invocation_with_recursive_context_expect_error(self):
        operator = OperatorWithWrongLoggerPlugin("operator")

        with self.assertRaises(RecursionError):
            operator.get_logger().info("This would introduce an endless recursion")

        with self.assertRaises(RecursionError):
            operator.get_logger().debug("This would introduce an endless recursion")

        with self.assertRaises(RecursionError):
            operator.get_logger().warn("This would introduce an endless recursion")

        with self.assertRaises(RecursionError):
            operator.get_logger().error("This would introduce an endless recursion")

    def test_get_dto_with_replicas_expect_ignored_replicas_in_dto(self):
        pipeline = TestPipelineWithOperator("pipeline")

        dto = pipeline.get_dto()

        self.assertEqual(2, len(dto.spec.nestedInstances))

        for nested_instance in dto.spec.nestedInstances:
            self.assertTrue(("operator_a" == nested_instance.name) or ("operator_b" == nested_instance.name))
