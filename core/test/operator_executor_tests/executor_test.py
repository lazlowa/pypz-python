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

from pypz.executors.commons import ExitCodes, ExecutionMode
from pypz.executors.operator.executor import OperatorExecutor
from core.test.operator_executor_tests.resources import TestPipeline


class OperatorExecutorTest(unittest.TestCase):

    def test_executor_without_error_expect_all_methods_called(self):
        pipeline = TestPipeline("pipeline")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.NoError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_commit)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_plugin_addon_init_expect_exception(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.addon.set_parameter("raise__pre_execution", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.FatalError.value, executor.execute())

        self.assertEqual(1, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.call_counter_init)
        self.assertEqual(0, pipeline.operator_a.call_counter_running)
        self.assertEqual(0, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_plugin_addon_shutdown_expect_exception(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.addon.set_parameter("raise__post_execution", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.FatalError.value, executor.execute())

        self.assertEqual(1, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_plugin_service_start_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.service_plugin.set_parameter("raise__on_service_start", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateServiceStartError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.call_counter_init)
        self.assertEqual(0, pipeline.operator_a.call_counter_running)
        self.assertEqual(0, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_plugin_service_shutdown_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.service_plugin.set_parameter("raise__on_service_shutdown", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateServiceShutdownError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_operation_init_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.set_parameter("raise__on_init", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateOperationInitError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(1, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(0, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_operation_running_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.set_parameter("raise__on_running", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateOperationError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(1, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_operation_shutdown_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.set_parameter("raise__on_shutdown", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateOperationShutdownError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(1, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_on_running_return_nothing_expect_finished(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.set_parameter("return__on_running", None)

        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.NoError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_commit)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_input_port_plugin_init_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.input_port.set_parameter("raise__on_port_open", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateOperationInitError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(1, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.call_counter_init)
        self.assertEqual(0, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)

        # Left here as a reminder that since the execution of plugin methods happens
        # along a set of plugins, the order is not guaranteed, so we cannot know
        # if other plugin on the same dependency level has been executed or not.
        # self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_init)
        # self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_input_port_plugin_close_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.input_port.set_parameter("raise__on_port_close", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateOperationShutdownError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(1, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_plugin_resource_creation_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.resource_handler.set_parameter("raise__on_resource_creation", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateResourceCreationError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.call_counter_init)
        self.assertEqual(0, pipeline.operator_a.call_counter_running)
        self.assertEqual(0, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_error_in_plugin_resource_deletion_expect_transition_to_error_state(self):
        pipeline = TestPipeline("pipeline")
        pipeline.operator_a.resource_handler.set_parameter("raise__on_resource_deletion", "Test Error")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.StateResourcesDeletionError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_run_mode_operation_wo_resource_deletion_state(self):
        pipeline = TestPipeline("pipeline")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.NoError.value, executor.execute(ExecutionMode.WithoutResourceDeletion))

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.call_counter_init)
        self.assertEqual(1, pipeline.operator_a.call_counter_running)
        self.assertEqual(1, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(1, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_run_mode_resource_creation_only_state(self):
        pipeline = TestPipeline("pipeline")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.NoError.value, executor.execute(ExecutionMode.ResourceCreationOnly))

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.call_counter_init)
        self.assertEqual(0, pipeline.operator_a.call_counter_running)
        self.assertEqual(0, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_run_mode_resource_deletion_only_state(self):
        pipeline = TestPipeline("pipeline")
        executor = OperatorExecutor(pipeline.operator_a)

        self.assertEqual(ExitCodes.NoError.value, executor.execute(ExecutionMode.ResourceDeletionOnly))

        self.assertEqual(0, pipeline.operator_a.addon.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.addon.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_start)
        self.assertEqual(1, pipeline.operator_a.addon.call_counter_addon_shutdown)

        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.service_plugin.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_start)
        self.assertEqual(1, pipeline.operator_a.service_plugin.call_counter_service_shutdown)

        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_interrupt)
        self.assertEqual(1, pipeline.operator_a.resource_handler.call_counter_resource_deletion)
        self.assertEqual(0, pipeline.operator_a.resource_handler.call_counter_resource_creation)

        self.assertEqual(0, pipeline.operator_a.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.call_counter_init)
        self.assertEqual(0, pipeline.operator_a.call_counter_running)
        self.assertEqual(0, pipeline.operator_a.call_counter_shutdown)

        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.input_port.call_counter_port_shutdown)

        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_error)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_interrupt)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_init)
        self.assertEqual(0, pipeline.operator_a.output_port.call_counter_port_shutdown)

    def test_executor_with_dependent_plugins_expect_correct_execution_order(self):
        pipeline = TestPipeline("pipeline")
        
        pipeline.operator_b.service_plugin_4.depends_on(pipeline.operator_b.service_plugin_3)
        pipeline.operator_b.service_plugin_3.depends_on(pipeline.operator_b.service_plugin_2)
        pipeline.operator_b.service_plugin_2.depends_on(pipeline.operator_b.service_plugin_1)
        pipeline.operator_b.service_plugin_1.depends_on(pipeline.operator_b.service_plugin_0)
        pipeline.operator_b.resource_handler_4.depends_on(pipeline.operator_b.resource_handler_3)
        pipeline.operator_b.resource_handler_3.depends_on(pipeline.operator_b.resource_handler_2)
        pipeline.operator_b.resource_handler_2.depends_on(pipeline.operator_b.resource_handler_1)
        pipeline.operator_b.resource_handler_1.depends_on(pipeline.operator_b.resource_handler_0)
        pipeline.operator_b.input_port_4.depends_on(pipeline.operator_b.input_port_3)
        pipeline.operator_b.input_port_3.depends_on(pipeline.operator_b.input_port_2)
        pipeline.operator_b.input_port_2.depends_on(pipeline.operator_b.input_port_1)
        pipeline.operator_b.input_port_1.depends_on(pipeline.operator_b.input_port_0)
        pipeline.operator_b.output_port_4.depends_on(pipeline.operator_b.output_port_3)
        pipeline.operator_b.output_port_3.depends_on(pipeline.operator_b.output_port_2)
        pipeline.operator_b.output_port_2.depends_on(pipeline.operator_b.output_port_1)
        pipeline.operator_b.output_port_1.depends_on(pipeline.operator_b.output_port_0)
        
        executor = OperatorExecutor(pipeline.operator_b)

        self.assertEqual(ExitCodes.NoError.value, executor.execute())

        self.assertEqual(0, pipeline.operator_b.service_plugin_0.init_order_idx)
        self.assertEqual(1, pipeline.operator_b.service_plugin_1.init_order_idx)
        self.assertEqual(2, pipeline.operator_b.service_plugin_2.init_order_idx)
        self.assertEqual(3, pipeline.operator_b.service_plugin_3.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.service_plugin_4.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.service_plugin_0.shutdown_order_idx)
        self.assertEqual(3, pipeline.operator_b.service_plugin_1.shutdown_order_idx)
        self.assertEqual(2, pipeline.operator_b.service_plugin_2.shutdown_order_idx)
        self.assertEqual(1, pipeline.operator_b.service_plugin_3.shutdown_order_idx)
        self.assertEqual(0, pipeline.operator_b.service_plugin_4.shutdown_order_idx)

        self.assertEqual(0, pipeline.operator_b.resource_handler_0.init_order_idx)
        self.assertEqual(1, pipeline.operator_b.resource_handler_1.init_order_idx)
        self.assertEqual(2, pipeline.operator_b.resource_handler_2.init_order_idx)
        self.assertEqual(3, pipeline.operator_b.resource_handler_3.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.resource_handler_4.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.resource_handler_0.shutdown_order_idx)
        self.assertEqual(3, pipeline.operator_b.resource_handler_1.shutdown_order_idx)
        self.assertEqual(2, pipeline.operator_b.resource_handler_2.shutdown_order_idx)
        self.assertEqual(1, pipeline.operator_b.resource_handler_3.shutdown_order_idx)
        self.assertEqual(0, pipeline.operator_b.resource_handler_4.shutdown_order_idx)

        self.assertEqual(0, pipeline.operator_b.input_port_0.init_order_idx)
        self.assertEqual(1, pipeline.operator_b.input_port_1.init_order_idx)
        self.assertEqual(2, pipeline.operator_b.input_port_2.init_order_idx)
        self.assertEqual(3, pipeline.operator_b.input_port_3.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.input_port_4.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.input_port_0.shutdown_order_idx)
        self.assertEqual(3, pipeline.operator_b.input_port_1.shutdown_order_idx)
        self.assertEqual(2, pipeline.operator_b.input_port_2.shutdown_order_idx)
        self.assertEqual(1, pipeline.operator_b.input_port_3.shutdown_order_idx)
        self.assertEqual(0, pipeline.operator_b.input_port_4.shutdown_order_idx)

        self.assertEqual(0, pipeline.operator_b.output_port_0.init_order_idx)
        self.assertEqual(1, pipeline.operator_b.output_port_1.init_order_idx)
        self.assertEqual(2, pipeline.operator_b.output_port_2.init_order_idx)
        self.assertEqual(3, pipeline.operator_b.output_port_3.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.output_port_4.init_order_idx)
        self.assertEqual(4, pipeline.operator_b.output_port_0.shutdown_order_idx)
        self.assertEqual(3, pipeline.operator_b.output_port_1.shutdown_order_idx)
        self.assertEqual(2, pipeline.operator_b.output_port_2.shutdown_order_idx)
        self.assertEqual(1, pipeline.operator_b.output_port_3.shutdown_order_idx)
        self.assertEqual(0, pipeline.operator_b.output_port_4.shutdown_order_idx)

