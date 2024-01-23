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

from pypz.sniffer.utils import retrieve_operator_paths, is_sublist, order_operators_by_connections
from sniffer.test.resources import TestPipelineWithSimpleConnections, TestPipelineWithBranchingConnections, \
    TestPipelineWithCircularDependentOperators, TestPipelineWithMergingConnections, \
    TestPipelineWithBranchingAndMergingConnections, TestPipelineWithBranchingAndMergingConnectionsWithMultipleOutputs, \
    TestPipelineWithBranchingAndMergingConnectionsWithAdditionalOutputs


class GraphResolutionTest(unittest.TestCase):

    def test_is_sublist_cases(self):
        self.assertTrue(is_sublist([], []))
        self.assertTrue(is_sublist([], [0]))
        self.assertTrue(is_sublist([0], [0]))
        self.assertTrue(is_sublist([0], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([0, 1], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([0, 1, 2], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([0, 1, 2, 3], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([0, 1, 2, 3, 4], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([4], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([3, 4], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([0, 1, 2, 3, 4], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([2, 3, 4], [0, 1, 2, 3, 4]))
        self.assertTrue(is_sublist([1, 2, 3, 4], [0, 1, 2, 3, 4]))

        self.assertFalse(is_sublist([0], []))
        self.assertFalse(is_sublist([0, 1], [0]))
        self.assertFalse(is_sublist([0, 0], [0, 1, 2, 3, 4]))
        self.assertFalse(is_sublist([0, 2], [0, 1, 2, 3, 4]))
        self.assertFalse(is_sublist([4, 3], [0, 1, 2, 3, 4]))
        self.assertFalse(is_sublist(["3", "4"], [0, 1, 2, 3, 4]))

    def test_path_retrieval_with_normal_pipeline(self):
        pipeline = TestPipelineWithSimpleConnections("pipeline")

        path = retrieve_operator_paths(pipeline.op_a)

        self.assertEqual(1, len(path))
        self.assertEqual(4, len(path[0]))
        self.assertEqual([pipeline.op_a, pipeline.op_b, pipeline.op_c, pipeline.op_d], path[0])

        path = retrieve_operator_paths(pipeline.op_b)

        self.assertEqual(1, len(path))
        self.assertEqual(3, len(path[0]))
        self.assertEqual([pipeline.op_b, pipeline.op_c, pipeline.op_d], path[0])

        path = retrieve_operator_paths(pipeline.op_c)

        self.assertEqual(1, len(path))
        self.assertEqual(2, len(path[0]))
        self.assertEqual([pipeline.op_c, pipeline.op_d], path[0])

        path = retrieve_operator_paths(pipeline.op_d)

        self.assertEqual(1, len(path))
        self.assertEqual(1, len(path[0]))
        self.assertEqual([pipeline.op_d], path[0])

    def test_path_retrieval_with_branching_pipeline(self):
        pipeline = TestPipelineWithBranchingConnections("pipeline")

        path = retrieve_operator_paths(pipeline.op_a)

        self.assertEqual(4, len(path))
        self.assertIn([pipeline.op_a, pipeline.op_b, pipeline.op_d], path)
        self.assertIn([pipeline.op_a, pipeline.op_b, pipeline.op_e], path)
        self.assertIn([pipeline.op_a, pipeline.op_c, pipeline.op_f], path)
        self.assertIn([pipeline.op_a, pipeline.op_c, pipeline.op_g], path)

        path = retrieve_operator_paths(pipeline.op_b)

        self.assertEqual(2, len(path))
        self.assertIn([pipeline.op_b, pipeline.op_d], path)
        self.assertIn([pipeline.op_b, pipeline.op_e], path)

        path = retrieve_operator_paths(pipeline.op_c)

        self.assertEqual(2, len(path))
        self.assertIn([pipeline.op_c, pipeline.op_f], path)
        self.assertIn([pipeline.op_c, pipeline.op_g], path)

    def test_path_retrieval_with_merging_pipeline(self):
        pipeline = TestPipelineWithMergingConnections("pipeline")

        path = retrieve_operator_paths(pipeline.op_a)

        self.assertEqual(1, len(path))
        self.assertEqual([pipeline.op_a], path[0])

        path = retrieve_operator_paths(pipeline.op_b)

        self.assertEqual(1, len(path))
        self.assertEqual([pipeline.op_b, pipeline.op_a], path[0])

        path = retrieve_operator_paths(pipeline.op_c)

        self.assertEqual(1, len(path))
        self.assertEqual([pipeline.op_c, pipeline.op_a], path[0])

        path = retrieve_operator_paths(pipeline.op_d)

        self.assertEqual(1, len(path))
        self.assertEqual([pipeline.op_d, pipeline.op_b, pipeline.op_a], path[0])

        path = retrieve_operator_paths(pipeline.op_e)

        self.assertEqual(1, len(path))
        self.assertEqual([pipeline.op_e, pipeline.op_b, pipeline.op_a], path[0])

        path = retrieve_operator_paths(pipeline.op_f)

        self.assertEqual(1, len(path))
        self.assertEqual([pipeline.op_f, pipeline.op_c, pipeline.op_a], path[0])

        path = retrieve_operator_paths(pipeline.op_g)

        self.assertEqual(1, len(path))
        self.assertEqual([pipeline.op_g, pipeline.op_c, pipeline.op_a], path[0])

    def test_path_retrieval_with_pipeline_with_circular_dependent_operators(self):
        pipeline = TestPipelineWithCircularDependentOperators("pipeline")

        path = retrieve_operator_paths(pipeline.op_a)

        self.assertEqual(1, len(path))
        self.assertEqual(4, len(path[0]))
        self.assertEqual([pipeline.op_a, pipeline.op_b, pipeline.op_c, pipeline.op_d], path[0])

        path = retrieve_operator_paths(pipeline.op_b)

        self.assertEqual(1, len(path))
        self.assertEqual(4, len(path[0]))
        self.assertEqual([pipeline.op_b, pipeline.op_c, pipeline.op_d, pipeline.op_a], path[0])

        path = retrieve_operator_paths(pipeline.op_c)

        self.assertEqual(1, len(path))
        self.assertEqual(4, len(path[0]))
        self.assertEqual([pipeline.op_c, pipeline.op_d, pipeline.op_a, pipeline.op_b], path[0])

        path = retrieve_operator_paths(pipeline.op_d)

        self.assertEqual(1, len(path))
        self.assertEqual(4, len(path[0]))
        self.assertEqual([pipeline.op_d, pipeline.op_a, pipeline.op_b, pipeline.op_c], path[0])

    def test_order_operators_by_connections_with_normal_pipeline(self):
        pipeline = TestPipelineWithSimpleConnections("pipeline")

        dependency_levels = order_operators_by_connections(pipeline)

        self.assertEqual(4, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[0]))
        self.assertIn(pipeline.op_a, dependency_levels[0])

        self.assertEqual(1, len(dependency_levels[1]))
        self.assertIn(pipeline.op_b, dependency_levels[1])

        self.assertEqual(1, len(dependency_levels[2]))
        self.assertIn(pipeline.op_c, dependency_levels[2])

        self.assertEqual(1, len(dependency_levels[3]))
        self.assertIn(pipeline.op_d, dependency_levels[3])

    def test_order_operators_by_connections_pipeline_with_circular_dependent_operators(self):
        pipeline = TestPipelineWithCircularDependentOperators("pipeline")

        dependency_levels = order_operators_by_connections(pipeline)

        self.assertEqual(4, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[0]))
        self.assertIn(pipeline.op_a, dependency_levels[0])

        self.assertEqual(1, len(dependency_levels[1]))
        self.assertIn(pipeline.op_b, dependency_levels[1])

        self.assertEqual(1, len(dependency_levels[2]))
        self.assertIn(pipeline.op_c, dependency_levels[2])

        self.assertEqual(1, len(dependency_levels[3]))
        self.assertIn(pipeline.op_d, dependency_levels[3])

    def test_order_operators_by_connections_with_branching_pipeline(self):
        pipeline = TestPipelineWithBranchingConnections("pipeline")

        dependency_levels = order_operators_by_connections(pipeline)

        self.assertEqual(3, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[0]))
        self.assertIn(pipeline.op_a, dependency_levels[0])

        self.assertEqual(2, len(dependency_levels[1]))
        self.assertIn(pipeline.op_b, dependency_levels[1])
        self.assertIn(pipeline.op_c, dependency_levels[1])

        self.assertEqual(4, len(dependency_levels[2]))
        self.assertIn(pipeline.op_d, dependency_levels[2])
        self.assertIn(pipeline.op_e, dependency_levels[2])
        self.assertIn(pipeline.op_f, dependency_levels[2])
        self.assertIn(pipeline.op_g, dependency_levels[2])

    def test_order_operators_by_connections_with_merging_pipeline(self):
        pipeline = TestPipelineWithMergingConnections("pipeline")

        dependency_levels = order_operators_by_connections(pipeline)

        self.assertEqual(3, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[2]))
        self.assertIn(pipeline.op_a, dependency_levels[2])

        self.assertEqual(2, len(dependency_levels[1]))
        self.assertIn(pipeline.op_b, dependency_levels[1])
        self.assertIn(pipeline.op_c, dependency_levels[1])

        self.assertEqual(4, len(dependency_levels[0]))
        self.assertIn(pipeline.op_d, dependency_levels[0])
        self.assertIn(pipeline.op_e, dependency_levels[0])
        self.assertIn(pipeline.op_f, dependency_levels[0])
        self.assertIn(pipeline.op_g, dependency_levels[0])

    def test_order_operators_by_connections_with_branching_and_merging_pipeline(self):
        pipeline = TestPipelineWithBranchingAndMergingConnections("pipeline")

        dependency_levels = order_operators_by_connections(pipeline)

        self.assertEqual(5, len(dependency_levels))

        self.assertEqual(2, len(dependency_levels[0]))
        self.assertIn(pipeline.op_a, dependency_levels[0])
        self.assertIn(pipeline.op_b, dependency_levels[0])

        self.assertEqual(1, len(dependency_levels[1]))
        self.assertIn(pipeline.op_c, dependency_levels[1])

        self.assertEqual(2, len(dependency_levels[2]))
        self.assertIn(pipeline.op_d, dependency_levels[2])
        self.assertIn(pipeline.op_e, dependency_levels[2])

        self.assertEqual(1, len(dependency_levels[3]))
        self.assertIn(pipeline.op_f, dependency_levels[3])

        self.assertEqual(2, len(dependency_levels[4]))
        self.assertIn(pipeline.op_g, dependency_levels[4])
        self.assertIn(pipeline.op_h, dependency_levels[4])

    def test_order_operators_by_connections_with_branching_and_merging_pipeline_with_multiple_outputs(self):
        pipeline = TestPipelineWithBranchingAndMergingConnectionsWithMultipleOutputs("pipeline")

        dependency_levels = order_operators_by_connections(pipeline)

        self.assertEqual(5, len(dependency_levels))

        self.assertEqual(2, len(dependency_levels[0]))
        self.assertIn(pipeline.op_a, dependency_levels[0])
        self.assertIn(pipeline.op_b, dependency_levels[0])

        self.assertEqual(1, len(dependency_levels[1]))
        self.assertIn(pipeline.op_c, dependency_levels[1])

        self.assertEqual(2, len(dependency_levels[2]))
        self.assertIn(pipeline.op_d, dependency_levels[2])
        self.assertIn(pipeline.op_e, dependency_levels[2])

        self.assertEqual(1, len(dependency_levels[3]))
        self.assertIn(pipeline.op_f, dependency_levels[3])

        self.assertEqual(2, len(dependency_levels[4]))
        self.assertIn(pipeline.op_g, dependency_levels[4])
        self.assertIn(pipeline.op_h, dependency_levels[4])

    def test_order_operators_by_connections_with_branching_and_merging_pipeline_with_additional_outputs(self):
        pipeline = TestPipelineWithBranchingAndMergingConnectionsWithAdditionalOutputs("pipeline")

        dependency_levels = order_operators_by_connections(pipeline)

        self.assertEqual(5, len(dependency_levels))

        self.assertEqual(2, len(dependency_levels[0]))
        self.assertIn(pipeline.op_a, dependency_levels[0])
        self.assertIn(pipeline.op_b, dependency_levels[0])

        self.assertEqual(1, len(dependency_levels[1]))
        self.assertIn(pipeline.op_c, dependency_levels[1])

        self.assertEqual(2, len(dependency_levels[2]))
        self.assertIn(pipeline.op_d, dependency_levels[2])
        self.assertIn(pipeline.op_e, dependency_levels[2])

        self.assertEqual(1, len(dependency_levels[3]))
        self.assertIn(pipeline.op_f, dependency_levels[3])

        self.assertEqual(2, len(dependency_levels[4]))
        self.assertIn(pipeline.op_g, dependency_levels[4])
        self.assertIn(pipeline.op_h, dependency_levels[4])
