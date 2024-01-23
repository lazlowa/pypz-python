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
import os
import unittest

from pypz.executors.commons import ExecutionMode
from pypz.executors.operator.context import ExecutionContext
from pypz.executors.operator.executor import OperatorExecutor
from pypz.core.specs.instance import Instance
from pypz.core.specs.plugin import ServicePlugin, ResourceHandlerPlugin, InputPortPlugin, OutputPortPlugin, PortPlugin, \
    Plugin
from core.test.operator_executor_tests.resources import TestPipeline


class ExecutionContextTest(unittest.TestCase):

    def test_context_with_runtime_template_parameter(self):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        pipeline = TestPipeline("pipeline")

        pipeline.operator_b.set_parameter("#test_env_var", "$(env:PYPZ_TEST_ENV_VAR)")
        pipeline.operator_b.set_parameter("#test_env_var_list", ["$(env:PYPZ_TEST_ENV_VAR)"])
        pipeline.operator_b.set_parameter("#test_env_var_set", {"$(env:PYPZ_TEST_ENV_VAR)"})
        pipeline.operator_b.set_parameter("#test_env_var_dict", {"env": "$(env:PYPZ_TEST_ENV_VAR)"})

        self.assertEqual("$(env:PYPZ_TEST_ENV_VAR)", pipeline.operator_b.get_parameter("test_env_var"))
        self.assertEqual("$(env:PYPZ_TEST_ENV_VAR)", pipeline.operator_b.service_plugin_0.get_parameter("test_env_var"))
        self.assertEqual(["$(env:PYPZ_TEST_ENV_VAR)"], pipeline.operator_b.service_plugin_0.get_parameter("test_env_var_list"))
        self.assertEqual({"$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.service_plugin_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.service_plugin_0.get_parameter("test_env_var_dict"))

        self.assertEqual("$(env:PYPZ_TEST_ENV_VAR)", pipeline.operator_b.resource_handler_0.get_parameter("test_env_var"))
        self.assertEqual(["$(env:PYPZ_TEST_ENV_VAR)"], pipeline.operator_b.resource_handler_0.get_parameter("test_env_var_list"))
        self.assertEqual({"$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.resource_handler_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.resource_handler_0.get_parameter("test_env_var_dict"))

        self.assertEqual("$(env:PYPZ_TEST_ENV_VAR)", pipeline.operator_b.input_port_0.get_parameter("test_env_var"))
        self.assertEqual(["$(env:PYPZ_TEST_ENV_VAR)"], pipeline.operator_b.input_port_0.get_parameter("test_env_var_list"))
        self.assertEqual({"$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.input_port_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.input_port_0.get_parameter("test_env_var_dict"))

        self.assertEqual("$(env:PYPZ_TEST_ENV_VAR)", pipeline.operator_b.output_port_0.get_parameter("test_env_var"))
        self.assertEqual(["$(env:PYPZ_TEST_ENV_VAR)"], pipeline.operator_b.output_port_0.get_parameter("test_env_var_list"))
        self.assertEqual({"$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.output_port_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "$(env:PYPZ_TEST_ENV_VAR)"}, pipeline.operator_b.output_port_0.get_parameter("test_env_var_dict"))

        OperatorExecutor(pipeline.operator_b)

        self.assertEqual("testValue", pipeline.operator_b.get_parameter("test_env_var"))
        self.assertEqual("testValue", pipeline.operator_b.service_plugin_0.get_parameter("test_env_var"))
        self.assertEqual(["testValue"], pipeline.operator_b.service_plugin_0.get_parameter("test_env_var_list"))
        self.assertEqual({"testValue"}, pipeline.operator_b.service_plugin_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "testValue"}, pipeline.operator_b.service_plugin_0.get_parameter("test_env_var_dict"))

        self.assertEqual("testValue", pipeline.operator_b.resource_handler_0.get_parameter("test_env_var"))
        self.assertEqual(["testValue"], pipeline.operator_b.resource_handler_0.get_parameter("test_env_var_list"))
        self.assertEqual({"testValue"}, pipeline.operator_b.resource_handler_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "testValue"}, pipeline.operator_b.resource_handler_0.get_parameter("test_env_var_dict"))

        self.assertEqual("testValue", pipeline.operator_b.input_port_0.get_parameter("test_env_var"))
        self.assertEqual(["testValue"], pipeline.operator_b.input_port_0.get_parameter("test_env_var_list"))
        self.assertEqual({"testValue"}, pipeline.operator_b.input_port_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "testValue"}, pipeline.operator_b.input_port_0.get_parameter("test_env_var_dict"))

        self.assertEqual("testValue", pipeline.operator_b.output_port_0.get_parameter("test_env_var"))
        self.assertEqual(["testValue"], pipeline.operator_b.output_port_0.get_parameter("test_env_var_list"))
        self.assertEqual({"testValue"}, pipeline.operator_b.output_port_0.get_parameter("test_env_var_set"))
        self.assertEqual({"env": "testValue"}, pipeline.operator_b.output_port_0.get_parameter("test_env_var_dict"))

    def test_context_with_independent_plugins_of_different_types(self):
        pipeline = TestPipeline("pipeline")

        context = ExecutionContext(pipeline.operator_b, ExecutionMode.Standard)

        self.assertEqual(20, len(context.get_plugin_instances_by_type(Instance)))
        self.assertEqual(20, len(context.get_plugin_instances_by_type(Plugin)))

        self.assertEqual(5, len(context.get_plugin_instances_by_type(ServicePlugin)))
        self.assertTrue(pipeline.operator_b.service_plugin_0 in context.get_plugin_instances_by_type(ServicePlugin))
        self.assertTrue(pipeline.operator_b.service_plugin_1 in context.get_plugin_instances_by_type(ServicePlugin))
        self.assertTrue(pipeline.operator_b.service_plugin_2 in context.get_plugin_instances_by_type(ServicePlugin))
        self.assertTrue(pipeline.operator_b.service_plugin_3 in context.get_plugin_instances_by_type(ServicePlugin))
        self.assertTrue(pipeline.operator_b.service_plugin_4 in context.get_plugin_instances_by_type(ServicePlugin))

        self.assertEqual(1, len(context.get_dependency_graph_by_type(ServicePlugin)))
        self.assertEqual(5, len(context.get_dependency_graph_by_type(ServicePlugin)[0]))
        self.assertTrue(pipeline.operator_b.service_plugin_0 in context.get_dependency_graph_by_type(ServicePlugin)[0])
        self.assertTrue(pipeline.operator_b.service_plugin_1 in context.get_dependency_graph_by_type(ServicePlugin)[0])
        self.assertTrue(pipeline.operator_b.service_plugin_2 in context.get_dependency_graph_by_type(ServicePlugin)[0])
        self.assertTrue(pipeline.operator_b.service_plugin_3 in context.get_dependency_graph_by_type(ServicePlugin)[0])
        self.assertTrue(pipeline.operator_b.service_plugin_4 in context.get_dependency_graph_by_type(ServicePlugin)[0])

        self.assertEqual(5, len(context.get_plugin_instances_by_type(ResourceHandlerPlugin)))
        self.assertTrue(pipeline.operator_b.resource_handler_0 in context.get_plugin_instances_by_type(ResourceHandlerPlugin))
        self.assertTrue(pipeline.operator_b.resource_handler_1 in context.get_plugin_instances_by_type(ResourceHandlerPlugin))
        self.assertTrue(pipeline.operator_b.resource_handler_2 in context.get_plugin_instances_by_type(ResourceHandlerPlugin))
        self.assertTrue(pipeline.operator_b.resource_handler_3 in context.get_plugin_instances_by_type(ResourceHandlerPlugin))
        self.assertTrue(pipeline.operator_b.resource_handler_4 in context.get_plugin_instances_by_type(ResourceHandlerPlugin))

        self.assertEqual(1, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)))
        self.assertEqual(5, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0]))
        self.assertTrue(pipeline.operator_b.resource_handler_0 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0])
        self.assertTrue(pipeline.operator_b.resource_handler_1 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0])
        self.assertTrue(pipeline.operator_b.resource_handler_2 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0])
        self.assertTrue(pipeline.operator_b.resource_handler_3 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0])
        self.assertTrue(pipeline.operator_b.resource_handler_4 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0])

        self.assertEqual(5, len(context.get_plugin_instances_by_type(InputPortPlugin)))
        self.assertTrue(pipeline.operator_b.input_port_0 in context.get_plugin_instances_by_type(InputPortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_1 in context.get_plugin_instances_by_type(InputPortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_2 in context.get_plugin_instances_by_type(InputPortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_3 in context.get_plugin_instances_by_type(InputPortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_4 in context.get_plugin_instances_by_type(InputPortPlugin))

        self.assertEqual(1, len(context.get_dependency_graph_by_type(InputPortPlugin)))
        self.assertEqual(5, len(context.get_dependency_graph_by_type(InputPortPlugin)[0]))
        self.assertTrue(pipeline.operator_b.input_port_0 in context.get_dependency_graph_by_type(InputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_1 in context.get_dependency_graph_by_type(InputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_2 in context.get_dependency_graph_by_type(InputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_3 in context.get_dependency_graph_by_type(InputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_4 in context.get_dependency_graph_by_type(InputPortPlugin)[0])

        self.assertEqual(5, len(context.get_plugin_instances_by_type(OutputPortPlugin)))
        self.assertTrue(pipeline.operator_b.output_port_0 in context.get_plugin_instances_by_type(OutputPortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_1 in context.get_plugin_instances_by_type(OutputPortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_2 in context.get_plugin_instances_by_type(OutputPortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_3 in context.get_plugin_instances_by_type(OutputPortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_4 in context.get_plugin_instances_by_type(OutputPortPlugin))

        self.assertEqual(1, len(context.get_dependency_graph_by_type(OutputPortPlugin)))
        self.assertEqual(5, len(context.get_dependency_graph_by_type(OutputPortPlugin)[0]))
        self.assertTrue(pipeline.operator_b.output_port_0 in context.get_dependency_graph_by_type(OutputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_1 in context.get_dependency_graph_by_type(OutputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_2 in context.get_dependency_graph_by_type(OutputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_3 in context.get_dependency_graph_by_type(OutputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_4 in context.get_dependency_graph_by_type(OutputPortPlugin)[0])

        self.assertEqual(10, len(context.get_plugin_instances_by_type(PortPlugin)))
        self.assertTrue(pipeline.operator_b.input_port_0 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_1 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_2 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_3 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.input_port_4 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_0 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_1 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_2 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_3 in context.get_plugin_instances_by_type(PortPlugin))
        self.assertTrue(pipeline.operator_b.output_port_4 in context.get_plugin_instances_by_type(PortPlugin))

        self.assertEqual(1, len(context.get_dependency_graph_by_type(PortPlugin)))
        self.assertEqual(10, len(context.get_dependency_graph_by_type(PortPlugin)[0]))
        self.assertTrue(pipeline.operator_b.input_port_0 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_1 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_2 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_3 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_4 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_0 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_1 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_2 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_3 in context.get_dependency_graph_by_type(PortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_4 in context.get_dependency_graph_by_type(PortPlugin)[0])

    def test_context_with_dependent_plugins_of_different_types(self):
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

        context = ExecutionContext(pipeline.operator_b, ExecutionMode.Standard)

        self.assertEqual(5, len(context.get_dependency_graph_by_type(ServicePlugin)))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ServicePlugin)[0]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ServicePlugin)[1]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ServicePlugin)[2]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ServicePlugin)[3]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ServicePlugin)[4]))
        self.assertTrue(pipeline.operator_b.service_plugin_0 in context.get_dependency_graph_by_type(ServicePlugin)[0])
        self.assertTrue(pipeline.operator_b.service_plugin_1 in context.get_dependency_graph_by_type(ServicePlugin)[1])
        self.assertTrue(pipeline.operator_b.service_plugin_2 in context.get_dependency_graph_by_type(ServicePlugin)[2])
        self.assertTrue(pipeline.operator_b.service_plugin_3 in context.get_dependency_graph_by_type(ServicePlugin)[3])
        self.assertTrue(pipeline.operator_b.service_plugin_4 in context.get_dependency_graph_by_type(ServicePlugin)[4])

        self.assertEqual(5, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)[1]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)[2]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)[3]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(ResourceHandlerPlugin)[4]))
        self.assertTrue(pipeline.operator_b.resource_handler_0 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[0])
        self.assertTrue(pipeline.operator_b.resource_handler_1 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[1])
        self.assertTrue(pipeline.operator_b.resource_handler_2 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[2])
        self.assertTrue(pipeline.operator_b.resource_handler_3 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[3])
        self.assertTrue(pipeline.operator_b.resource_handler_4 in context.get_dependency_graph_by_type(ResourceHandlerPlugin)[4])

        self.assertEqual(5, len(context.get_dependency_graph_by_type(InputPortPlugin)))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(InputPortPlugin)[0]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(InputPortPlugin)[1]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(InputPortPlugin)[2]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(InputPortPlugin)[3]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(InputPortPlugin)[4]))
        self.assertTrue(pipeline.operator_b.input_port_0 in context.get_dependency_graph_by_type(InputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.input_port_1 in context.get_dependency_graph_by_type(InputPortPlugin)[1])
        self.assertTrue(pipeline.operator_b.input_port_2 in context.get_dependency_graph_by_type(InputPortPlugin)[2])
        self.assertTrue(pipeline.operator_b.input_port_3 in context.get_dependency_graph_by_type(InputPortPlugin)[3])
        self.assertTrue(pipeline.operator_b.input_port_4 in context.get_dependency_graph_by_type(InputPortPlugin)[4])

        self.assertEqual(5, len(context.get_dependency_graph_by_type(OutputPortPlugin)))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(OutputPortPlugin)[0]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(OutputPortPlugin)[1]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(OutputPortPlugin)[2]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(OutputPortPlugin)[3]))
        self.assertEqual(1, len(context.get_dependency_graph_by_type(OutputPortPlugin)[4]))
        self.assertTrue(pipeline.operator_b.output_port_0 in context.get_dependency_graph_by_type(OutputPortPlugin)[0])
        self.assertTrue(pipeline.operator_b.output_port_1 in context.get_dependency_graph_by_type(OutputPortPlugin)[1])
        self.assertTrue(pipeline.operator_b.output_port_2 in context.get_dependency_graph_by_type(OutputPortPlugin)[2])
        self.assertTrue(pipeline.operator_b.output_port_3 in context.get_dependency_graph_by_type(OutputPortPlugin)[3])
        self.assertTrue(pipeline.operator_b.output_port_4 in context.get_dependency_graph_by_type(OutputPortPlugin)[4])
