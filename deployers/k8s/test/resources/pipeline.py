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
import time
from typing import Any, Optional

from pypz.core.commons.parameters import OptionalParameter
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.operators.k8s import KubernetesOperator
from pypz.plugins.loggers.default import DefaultLoggerPlugin


class TestOperator(Operator):

    error = OptionalParameter(bool)
    env_var = OptionalParameter(str)

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")
        self.error = False
        self.env_var = ""

    def _on_init(self) -> bool:
        print("Init")
        if self.env_var:
            print(f"{self.env_var}={os.getenv(self.env_var)}")
        return True

    def _on_running(self) -> Optional[bool]:
        if self.error:
            raise ValueError
        return True

    def _on_shutdown(self) -> bool:
        return True

    def _on_interrupt(self, system_signal: int = None) -> None:
        pass

    def _on_error(self, source: Any, exception: Exception) -> None:
        pass


class TestPipeline(Pipeline):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.operator_a = TestOperator()
        self.operator_a.set_parameter("replicationFactor", 1)

        self.operator_b = TestOperator()
        self.operator_c = TestOperator()
        self.operator_d = TestOperator()


class TestKubernetesOperator(KubernetesOperator):
    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.logger = DefaultLoggerPlugin()
        self.logger.set_parameter("logLevel", "DEBUG")

    def _on_init(self) -> bool:
        return True

    def _on_running(self) -> Optional[bool]:
        time.sleep(10)
        return True

    def _on_shutdown(self) -> bool:
        return True

    def _on_interrupt(self, system_signal: int = None) -> None:
        pass

    def _on_error(self, source: Any, exception: Exception) -> None:
        pass


class TestKubernetesPipeline(Pipeline):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.op = TestKubernetesOperator()
        self.op.set_parameter(
            "readinessProbe",
            {
                "httpGet": {
                    "path": "/check",
                    "port": self.op.health_check.get_parameter("port"),
                },
                "initialDelaySeconds": 0,
                "periodSeconds": 5,
                "timeoutSeconds": 5,
                "failureThreshold": 1,
            },
        )
