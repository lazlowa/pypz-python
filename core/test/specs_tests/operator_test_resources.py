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
from typing import Any, Optional

from pypz.core.commons.parameters import OptionalParameter, RequiredParameter
from pypz.core.specs.misc import (
    BlankInputPortPlugin,
    BlankOperator,
    BlankOutputPortPlugin,
    BlankPlugin,
)
from pypz.core.specs.pipeline import Pipeline
from pypz.core.specs.plugin import LoggerPlugin


class ExtendedOutputPort(BlankOutputPortPlugin):
    req_str = RequiredParameter(str)
    _opt_str = RequiredParameter(str)

    req_int = RequiredParameter(int)
    _opt_int = OptionalParameter(int)

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.req_str = None
        self._opt_str = "default"
        self.req_int = None
        self._opt_int = 1234


class WrongLoggerPlugin(LoggerPlugin, BlankPlugin):

    def _info(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self.get_logger().info("This would introduce an endless recursion")

    def _error(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self.get_logger().info("This would introduce an endless recursion")

    def _warning(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self.get_logger().info("This would introduce an endless recursion")

    def _debug(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self.get_logger().info("This would introduce an endless recursion")


class TestOperatorWithPortPlugins(BlankOperator):

    param_a = OptionalParameter(int)
    param_b = OptionalParameter(int)
    param_c = OptionalParameter(int)
    param_d = OptionalParameter(int)
    param_e = OptionalParameter(int)

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.input_port = BlankInputPortPlugin()
        self.output_port = ExtendedOutputPort()

        self.param_a = 0
        self.param_b = 0
        self.param_c = 0
        self.param_d = 0
        self.param_e = 0


class OperatorWithWrongLoggerPlugin(BlankOperator):

    def __init__(self, name: str = None):
        super().__init__(name)
        self.wrong_logger_plugin = WrongLoggerPlugin()


class TestPipelineWithOperator(Pipeline):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.operator_a = TestOperatorWithPortPlugins()
        self.operator_a.set_parameter("replicationFactor", 5)

        self.operator_b = TestOperatorWithPortPlugins()

        self.operator_a.input_port.connect(self.operator_b.output_port)


p = TestPipelineWithOperator("pipeline")
# print(p.operator_a)
print("=================================================")

# print(p.operator_a_0)
# print(hash(p.operator_a_0))
# print(p.operator_a_0 == p.operator_a)
print(p.operator_a_0 != p.operator_a)
# print(p.operator_a == p.operator_a_0)
# print(p.operator_a_0 == p.operator_a_0)
# print(p.operator_a_0 == p.operator_a_1)
# print(p.get_full_name())  # Expected: pipeline
# print(p.operator_a.get_full_name())  # Expected: pipeline.operator_a
# print(p.operator_a_0.get_full_name())  # Expected: pipeline.operator_a_0
#
# print(p.operator_a.input_port.get_full_name())  # Expected # Expected: pipeline.operator_a.input_port
# print(p.operator_a_0.input_port.get_full_name())  # Expected # Expected: pipeline.operator_a_0.input_port
#
# odto = p.operator_a.get_dto()
# print(odto.name)  # Expected: pipeline.operator_a
# r1dto = p.operator_a_0.get_dto()
# print(r1dto.name)  # Expected: pipeline.operator_a_0


# pipeline name
# operator full name
# replica full name
# operator plugin full name
# replica plugin full name
# context from plugin

# Operator replica + plugin
# dto
# str
# eq
# hash
# group name
# principal
# is principal
# group index

# create from dto / string
# all the above from the created one

# replica creation / deletion -> param setting
