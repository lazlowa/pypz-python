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
from pypz.executors.commons import ExecutionMode
from pypz.executors.operator.context import ExecutionContext
from pypz.executors.operator.signals import SignalOperationStart, SignalOperationStop, SignalServicesStop, \
    SignalServicesStart, SignalResourcesCreation, SignalResourcesDeletion, SignalOperationInit, SignalError, \
    SignalKill, SignalNoOp, SignalShutdown, SignalTerminate
from pypz.executors.operator.states import State
from pypz.core.specs.operator import Operator
import pypz.core.commons.utils
import unittest

from core.test.operator_executor_tests.resources import TestPipeline


# ============= Resources =============


class TestState1(State):

    def __init__(self, context: ExecutionContext,
                 executor_pool_size: int = None):
        super().__init__(context, executor_pool_size)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> None:
        pass


class TestState2(State):

    def __init__(self, context: ExecutionContext,
                 executor_pool_size: int = None):
        super().__init__(context, executor_pool_size)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> None:
        pass

# ============= Tests =============


class BaseStateTest(unittest.TestCase):

    def test_state_action_callable_with_service_plugin_expect_success(self):
        pipeline = TestPipeline("pipeline")
        action_callable = State.MethodWrapper(TestState1(ExecutionContext(pipeline.operator_a,
                                                                          ExecutionMode.Standard)),
                                              pipeline.operator_a.service_plugin,
                                              pipeline.operator_a.service_plugin._on_service_start)

        self.assertTrue(action_callable())
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

    def test_state_action_callable_with_operator_expect_success(self):
        pipeline = TestPipeline("pipeline")
        action_callable = State.MethodWrapper(TestState1(ExecutionContext(pipeline.operator_a,
                                                                          ExecutionMode.Standard)),
                                              pipeline.operator_a,
                                              pipeline.operator_a._on_running)

        self.assertTrue(action_callable())
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(0, pipeline.operator_a.call_counter_shutdown)

    def test_state_action_callable_with_operator_with_error_raised_expect_error(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.set_parameter("raise__on_running", "Test Error")
        action_callable = State.MethodWrapper(TestState1(ExecutionContext(pipeline.operator_a,
                                                                          ExecutionMode.Standard)),
                                              pipeline.operator_a,
                                              pipeline.operator_a._on_running)

        with self.assertRaises(AttributeError):
            action_callable()

    def test_transition_multi_registration_expect_error(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state1 = TestState1(context)
        state2 = TestState2(context)

        state1.set_transition(SignalOperationStart, state2)

        with self.assertRaises(AttributeError):
            state1.set_transition(SignalOperationStart, state2)

        state1.shutdown()
        state2.shutdown()

    def test_transition_handling_expect_success(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state1 = TestState1(context)
        state2 = TestState2(context)

        state1.set_transition(SignalOperationStart, state2)
        state1.set_transition(SignalOperationStop, state2)
        state1.set_transition(SignalServicesStart, state2)
        state1.set_transition(SignalServicesStop, state2)
        state1.set_transition(SignalResourcesCreation, state2)
        state1.set_transition(SignalResourcesDeletion, state2)
        state1.set_transition(SignalOperationInit, state2)
        state1.set_transition(SignalError, state2)
        state1.set_transition(SignalKill, state2)
        state1.set_transition(SignalNoOp, state2)
        state1.set_transition(SignalShutdown, state2)
        state1.set_transition(SignalTerminate, state2)

        self.assertEqual(12, len(state1.get_transitions()))

        self.assertEqual(state2, state1.on_signal_handling(SignalOperationStart()))
        self.assertEqual(state2, state1.on_signal_handling(SignalOperationStop()))
        self.assertEqual(state2, state1.on_signal_handling(SignalServicesStart()))
        self.assertEqual(state2, state1.on_signal_handling(SignalServicesStop()))
        self.assertEqual(state2, state1.on_signal_handling(SignalResourcesCreation()))
        self.assertEqual(state2, state1.on_signal_handling(SignalResourcesDeletion()))
        self.assertEqual(state2, state1.on_signal_handling(SignalOperationInit()))
        self.assertEqual(state2, state1.on_signal_handling(SignalError()))
        self.assertEqual(state2, state1.on_signal_handling(SignalKill()))
        self.assertEqual(state2, state1.on_signal_handling(SignalNoOp()))
        self.assertEqual(state2, state1.on_signal_handling(SignalShutdown()))
        self.assertEqual(state2, state1.on_signal_handling(SignalTerminate()))

        state1.shutdown()
        state2.shutdown()

    def test_unhandled_transition_expect_remaining_in_the_current_state(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state1 = TestState1(context)

        self.assertEqual(state1, state1.on_signal_handling(SignalOperationStart()))
        self.assertEqual(state1, state1.on_signal_handling(SignalOperationStop()))
        self.assertEqual(state1, state1.on_signal_handling(SignalServicesStart()))
        self.assertEqual(state1, state1.on_signal_handling(SignalServicesStop()))
        self.assertEqual(state1, state1.on_signal_handling(SignalResourcesCreation()))
        self.assertEqual(state1, state1.on_signal_handling(SignalResourcesDeletion()))
        self.assertEqual(state1, state1.on_signal_handling(SignalOperationInit()))
        self.assertEqual(state1, state1.on_signal_handling(SignalError()))
        self.assertEqual(state1, state1.on_signal_handling(SignalKill()))
        self.assertEqual(state1, state1.on_signal_handling(SignalNoOp()))
        self.assertEqual(state1, state1.on_signal_handling(SignalShutdown()))
        self.assertEqual(state1, state1.on_signal_handling(SignalTerminate()))

        state1.shutdown()

    def test_transition_handling_with_invalid_signal_type_expect_error(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state1 = TestState1(context)

        with self.assertRaises(AttributeError):
            self.assertEqual(state1, state1.on_signal_handling(SignalOperationStart))

        with self.assertRaises(AttributeError):
            self.assertEqual(state1, state1.on_signal_handling("invalid_signal"))

        state1.shutdown()

    def test_scheduler_with_return_true_expect_success(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state = TestState1(context)

        start_time = pypz.core.commons.utils.current_time_millis()
        self.assertTrue(state._schedule((Operator._on_init, {pipeline.operator_a})))
        self.assertTrue(1100 > (pypz.core.commons.utils.current_time_millis() - start_time))

        state.shutdown()

    def test_scheduler_with_return_false_expect_success(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state = TestState1(context)

        pipeline.operator_a.set_parameter("return__on_init", False)

        start_time = pypz.core.commons.utils.current_time_millis()
        self.assertFalse(state._schedule((Operator._on_init, {pipeline.operator_a})))
        self.assertTrue(1100 > (pypz.core.commons.utils.current_time_millis() - start_time))

        state.shutdown()

    def test_scheduler_with_repeated_calls_expect_no_execution(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state = TestState1(context)

        self.assertTrue(state._schedule((Operator._on_init, {pipeline.operator_a})))
        self.assertTrue(state._schedule((Operator._on_init, {pipeline.operator_a})))
        self.assertTrue(state._schedule((Operator._on_init, {pipeline.operator_a})))

        state.shutdown()

    def test_scheduler_with_invalid_return_type_expect_fail(self):
        pipeline = TestPipeline("pipeline")
        context = ExecutionContext(pipeline.operator_a, ExecutionMode.Standard)

        state = TestState1(context)

        pipeline.operator_a.set_parameter("return__on_init", None)

        with self.assertRaises(RuntimeError):
            state._schedule((Operator._on_init, {pipeline.operator_a}))

        state.shutdown()
