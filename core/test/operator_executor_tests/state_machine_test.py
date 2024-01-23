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
from pypz.executors.operator.signals import SignalNoOp, SignalResourcesCreation, SignalServicesStart, \
    SignalOperationInit, SignalOperationStart, SignalOperationStop, SignalServicesStop, SignalResourcesDeletion, \
    SignalKill, SignalTerminate, SignalError, SignalShutdown
from pypz.executors.operator.states import StateEntry, StateServiceStart, StateKilled, StateResourceCreation, \
    StateResourceDeletion, StateServiceShutdown, StateOperationInit, StateOperationRunning, StateOperationShutdown
from core.test.operator_executor_tests.resources import TestPipeline


class OperatorStateMachineTest(unittest.TestCase):

    def test_transition_from_state_entry_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        self.assertIsInstance(ex.get_current_state(), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateEntry)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateKilled)

    def test_transition_from_state_start_services_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_service_start

        self.assertIsInstance(ex.get_current_state(), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()),
                              StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()),
                              StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateServiceStart)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateServiceShutdown)

    def test_transition_from_state_resource_creation_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_resource_creation

        self.assertIsInstance(ex.get_current_state(), StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()),
                              StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()),
                              StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateResourceCreation)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateResourceDeletion)

    def test_transition_from_state_operation_init_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_operation_init

        self.assertIsInstance(ex.get_current_state(), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateOperationInit)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateOperationShutdown)

    def test_transition_from_state_operation_running_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_operation_running

        self.assertIsInstance(ex.get_current_state(), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()),
                              StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()),
                              StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateOperationRunning)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateOperationShutdown)

    def test_transition_from_state_operation_shutdown_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_operation_shutdown

        self.assertIsInstance(ex.get_current_state(), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()),
                              StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()),
                              StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateOperationShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateOperationShutdown)

    def test_transition_from_state_resource_deletion_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_resource_deletion

        self.assertIsInstance(ex.get_current_state(), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()),
                              StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()),
                              StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateResourceDeletion)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateResourceDeletion)

    def test_transition_from_state_service_shutdown_expect_success(self):
        pipeline = TestPipeline("pipeline")
        ex = OperatorExecutor(pipeline.operator_a)
        ex._OperatorExecutor__initialize()

        ex._OperatorExecutor__current_state = ex._OperatorExecutor__state_service_shutdown

        self.assertIsInstance(ex.get_current_state(), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalNoOp()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStart()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesCreation()),
                              StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationInit()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStart()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalOperationStop()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalResourcesDeletion()),
                              StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalServicesStop()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalKill()), StateKilled)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalTerminate()), StateServiceShutdown)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalError()), StateKilled)
        self.assertIsInstance(ex.get_current_state().on_signal_handling(SignalShutdown()), StateServiceShutdown)
