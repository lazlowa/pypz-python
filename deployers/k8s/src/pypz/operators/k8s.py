# =============================================================================
# Copyright (c) 2025 by Laszlo Anka. All rights reserved.
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
from abc import ABC

from pypz.core.specs.operator import Operator
from pypz.plugins.misc.health_check import HttpHealthCheckPlugin


class KubernetesOperator(Operator, ABC):
    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.health_check = HttpHealthCheckPlugin()
        """
        Including health check plugin by default.
        """

        # Configuring probes
        # ------------------

        # Startup probe to identify, if the operator has already started.
        # It is necessary to identify unhealthy scenario, if already
        # started, but not ready.
        self.set_parameter(
            "startupProbe",
            {
                "httpGet": {
                    "path": "/check",
                    "port": self.health_check.get_parameter("port"),
                },
                "periodSeconds": 1,
                "failureThreshold": 300,
            },
        )

        # Readiness probe to identify unhealthy scenarios. The reason for
        # using it instead of liveness is that it is simpler to detect.
        self.set_parameter(
            "readinessProbe",
            {
                "httpGet": {
                    "path": "/check",
                    "port": self.health_check.get_parameter("port"),
                },
                "initialDelaySeconds": 0,
                "periodSeconds": 30,
                "timeoutSeconds": 5,
                "failureThreshold": 20,
            },
        )
