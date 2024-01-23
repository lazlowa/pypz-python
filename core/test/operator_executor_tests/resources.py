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
import time
from typing import Optional

from pypz.core.commons.parameters import RequiredParameter
from pypz.core.specs.misc import BlankInputPortPlugin, BlankOutputPortPlugin, BlankPlugin, BlankOperator
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.core.specs.plugin import ServicePlugin, ResourceHandlerPlugin, ExtendedPlugin, PortPlugin, Plugin
from pypz.plugins.loggers.default import DefaultLoggerPlugin


class TestPluginBase(Plugin):

    ParameterPrefixReturnValue = "return_"
    ParameterPrefixSleep = "sleep_"
    ParameterPrefixRaiseError = "raise_"

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.call_counter_interrupt: int = 0
        self.call_counter_error: int = 0

        self.interrupted = False

    def _on_interrupt(self, system_signal: int = None) -> None:
        self.call_counter_interrupt += 1
        self.get_logger().warn(f"Interrupted by signal: {system_signal}")
        self.interrupted = True
        return self.control_handler(TestPluginBase._on_interrupt.__name__, None)

    def _on_error(self) -> None:
        self.call_counter_error += 1
        return self.control_handler(TestPluginBase._on_error.__name__, None)

    def control_handler(self, method_name: str, default_return_value=None):
        self.get_logger().debug(method_name)
        if self.has_parameter(TestPluginBase.ParameterPrefixRaiseError + method_name):
            raise AttributeError(self.get_parameter(TestPluginBase.ParameterPrefixRaiseError + method_name))

        if self.has_parameter(TestPluginBase.ParameterPrefixSleep + method_name):
            time.sleep(self.get_parameter(TestPluginBase.ParameterPrefixSleep + method_name))

        if self.has_parameter(TestPluginBase.ParameterPrefixReturnValue + method_name):
            return self.get_parameter(TestPluginBase.ParameterPrefixReturnValue + method_name)

        return default_return_value


class TestExtendedPlugin(TestPluginBase, ExtendedPlugin):
    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.call_counter_addon_start: int = 0
        self.call_counter_addon_shutdown: int = 0

    def _pre_execution(self) -> None:
        self.call_counter_addon_start += 1
        self.control_handler(TestExtendedPlugin._pre_execution.__name__, True)
        pass

    def _post_execution(self) -> None:
        self.call_counter_addon_shutdown += 1
        self.control_handler(TestExtendedPlugin._post_execution.__name__, True)
        pass


class TestInputPortPlugin(TestPluginBase, BlankInputPortPlugin):

    init_order_idx = 0
    shutdown_order_idx = 0

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.call_counter_port_init = 0
        self.call_counter_port_shutdown = 0
        self.call_counter_commit = 0

    def _on_port_open(self) -> bool:
        self.call_counter_port_init += 1
        self.init_order_idx = TestInputPortPlugin.init_order_idx
        TestInputPortPlugin.init_order_idx += 1
        return self.control_handler(PortPlugin._on_port_open.__name__, True)

    def _on_port_close(self) -> bool:
        self.call_counter_port_shutdown += 1
        self.shutdown_order_idx = TestInputPortPlugin.shutdown_order_idx
        TestInputPortPlugin.shutdown_order_idx += 1
        return self.control_handler(PortPlugin._on_port_close.__name__, True)

    def commit_current_read_offset(self) -> None:
        self.call_counter_commit += 1

    def can_retrieve(self) -> bool:
        return False


class TestOutputPortPlugin(TestPluginBase, BlankOutputPortPlugin):

    init_order_idx = 0
    shutdown_order_idx = 0

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.call_counter_port_init = 0
        self.call_counter_port_shutdown = 0

    def _on_port_open(self) -> bool:
        self.call_counter_port_init += 1
        self.init_order_idx = TestOutputPortPlugin.init_order_idx
        TestOutputPortPlugin.init_order_idx += 1
        return self.control_handler(PortPlugin._on_port_open.__name__, True)

    def _on_port_close(self) -> bool:
        self.call_counter_port_shutdown += 1
        self.shutdown_order_idx = TestOutputPortPlugin.shutdown_order_idx
        TestOutputPortPlugin.shutdown_order_idx += 1
        return self.control_handler(PortPlugin._on_port_close.__name__, True)


class TestResourceHandlerPlugin(TestPluginBase, ResourceHandlerPlugin):

    init_order_idx = 0
    shutdown_order_idx = 0

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.call_counter_resource_creation: int = 0
        self.call_counter_resource_deletion: int = 0

        self.init_order_idx = 0
        self.shutdown_order_idx = 0

    def _on_resource_creation(self) -> bool:
        self.call_counter_resource_creation += 1
        self.init_order_idx = TestResourceHandlerPlugin.init_order_idx
        TestResourceHandlerPlugin.init_order_idx += 1
        return self.control_handler(TestResourceHandlerPlugin._on_resource_creation.__name__, True)

    def _on_resource_deletion(self) -> bool:
        self.call_counter_resource_deletion += 1
        self.shutdown_order_idx = TestResourceHandlerPlugin.shutdown_order_idx
        TestResourceHandlerPlugin.shutdown_order_idx += 1
        return self.control_handler(TestResourceHandlerPlugin._on_resource_deletion.__name__, True)


class TestServicePlugin(TestPluginBase, ServicePlugin):

    init_order_idx = 0
    shutdown_order_idx = 0

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.call_counter_service_start: int = 0
        self.call_counter_service_shutdown: int = 0
        self.init_order_idx = 0
        self.shutdown_order_idx = 0

    def _on_service_start(self) -> bool:
        self.call_counter_service_start += 1
        self.init_order_idx = TestServicePlugin.init_order_idx
        TestServicePlugin.init_order_idx += 1
        return self.control_handler(TestServicePlugin._on_service_start.__name__, True)

    def _on_service_shutdown(self) -> bool:
        self.call_counter_service_shutdown += 1
        self.shutdown_order_idx = TestServicePlugin.shutdown_order_idx
        TestServicePlugin.shutdown_order_idx += 1
        return self.control_handler(TestServicePlugin._on_service_shutdown.__name__, True)


class TestServicePluginWithRequiredParam(ServicePlugin, BlankPlugin):

    req = RequiredParameter(str)

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

    def _on_service_start(self) -> bool:
        return True

    def _on_service_shutdown(self) -> bool:
        return True


class TestOperator(Operator):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.input_port: TestInputPortPlugin = TestInputPortPlugin()
        self.output_port: TestOutputPortPlugin = TestOutputPortPlugin()
        self.service_plugin: TestServicePlugin = TestServicePlugin()
        self.resource_handler: TestResourceHandlerPlugin = TestResourceHandlerPlugin()
        self.addon: TestExtendedPlugin = TestExtendedPlugin()

        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")

        self.call_counter_interrupt: int = 0
        self.call_counter_error: int = 0

        self.call_counter_init: int = 0
        self.call_counter_running: int = 0
        self.call_counter_shutdown: int = 0

        self.interrupted = False

    def _on_interrupt(self, system_signal: int = None) -> None:
        self.call_counter_interrupt += 1
        self.get_logger().warn(f"Interrupted by signal: {system_signal}")
        self.interrupted = True
        return self.control_handler(TestPluginBase._on_interrupt.__name__, None)

    def _on_error(self) -> None:
        self.call_counter_error += 1
        return self.control_handler(TestPluginBase._on_error.__name__, None)

    def _on_init(self) -> bool:
        self.call_counter_init += 1
        return self.control_handler(TestOperator._on_init.__name__, True)

    def _on_running(self) -> Optional[bool]:
        self.call_counter_running += 1
        return self.control_handler(TestOperator._on_running.__name__, True)

    def _on_shutdown(self) -> bool:
        self.call_counter_shutdown += 1
        return self.control_handler(TestOperator._on_shutdown.__name__, True)

    def control_handler(self, method_name: str, default_return_value=None):
        self.get_logger().debug(method_name)
        if self.has_parameter(TestPluginBase.ParameterPrefixRaiseError + method_name):
            raise AttributeError(self.get_parameter(TestPluginBase.ParameterPrefixRaiseError + method_name))

        if self.has_parameter(TestPluginBase.ParameterPrefixSleep + method_name):
            time.sleep(self.get_parameter(TestPluginBase.ParameterPrefixSleep + method_name))

        if self.has_parameter(TestPluginBase.ParameterPrefixReturnValue + method_name):
            return self.get_parameter(TestPluginBase.ParameterPrefixReturnValue + method_name)

        return default_return_value


class TestOperatorWithRequiredParameter(BlankOperator):

    req_str = RequiredParameter(str)

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)


class TestOperatorWithMultiplePluginsFromSameType(BlankOperator):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.service_plugin_0: TestServicePlugin = TestServicePlugin()
        self.service_plugin_1: TestServicePlugin = TestServicePlugin()
        self.service_plugin_2: TestServicePlugin = TestServicePlugin()
        self.service_plugin_3: TestServicePlugin = TestServicePlugin()
        self.service_plugin_4: TestServicePlugin = TestServicePlugin()

        self.resource_handler_0: TestResourceHandlerPlugin = TestResourceHandlerPlugin()
        self.resource_handler_1: TestResourceHandlerPlugin = TestResourceHandlerPlugin()
        self.resource_handler_2: TestResourceHandlerPlugin = TestResourceHandlerPlugin()
        self.resource_handler_3: TestResourceHandlerPlugin = TestResourceHandlerPlugin()
        self.resource_handler_4: TestResourceHandlerPlugin = TestResourceHandlerPlugin()

        self.input_port_0: TestInputPortPlugin = TestInputPortPlugin()
        self.input_port_1: TestInputPortPlugin = TestInputPortPlugin()
        self.input_port_2: TestInputPortPlugin = TestInputPortPlugin()
        self.input_port_3: TestInputPortPlugin = TestInputPortPlugin()
        self.input_port_4: TestInputPortPlugin = TestInputPortPlugin()

        self.output_port_0: TestOutputPortPlugin = TestOutputPortPlugin()
        self.output_port_1: TestOutputPortPlugin = TestOutputPortPlugin()
        self.output_port_2: TestOutputPortPlugin = TestOutputPortPlugin()
        self.output_port_3: TestOutputPortPlugin = TestOutputPortPlugin()
        self.output_port_4: TestOutputPortPlugin = TestOutputPortPlugin()


class TestPipeline(Pipeline):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.operator_a = TestOperator()
        self.operator_b = TestOperatorWithMultiplePluginsFromSameType()
