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

from pypz.executors.operator.executor import OperatorExecutor
from pypz.executors.operator.signals import SignalNoOp, SignalError, SignalOperationStop
from core.test.operator_executor_tests.resources import TestPipeline


class StateOperationProcessingTest(unittest.TestCase):

    def test_state_execution_results(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_operation_running

        self.assertIsInstance(ex.get_current_state().on_execute(), SignalOperationStop)

    def test_state_execution_results_with_unfinished_operator(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.set_parameter("return__on_running", False)
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_operation_running

        self.assertIsInstance(ex.get_current_state().on_execute(), SignalNoOp)

    def test_state_execution_results_with_error_raised_from_operator(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.set_parameter("raise__on_running", "Test Error")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_operation_running

        self.assertIsInstance(ex.get_current_state().on_execute(), SignalError)
