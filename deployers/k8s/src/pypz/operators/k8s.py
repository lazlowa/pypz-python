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

from pypz.core.commons.parameters import OptionalParameter
from pypz.core.specs.operator import Operator
from pypz.plugins.misc.health_check import HttpHealthCheckPlugin


class KubernetesOperator(Operator, ABC):
    image_pull_policy = OptionalParameter(str, alt_name="imagePullPolicy")
    restart_policy = OptionalParameter(str, alt_name="restartPolicy")
    env = OptionalParameter(list[dict], alt_name="env")
    env_from = OptionalParameter(list[dict], alt_name="envFrom")
    volume_mounts = OptionalParameter(list[dict], alt_name="volumeMounts")
    containers = OptionalParameter(list[dict], alt_name="containers")
    volumes = OptionalParameter(list[dict], alt_name="volumes")
    termination_grace_period_seconds = OptionalParameter(
        int, alt_name="terminationGracePeriodSeconds"
    )
    service_account_name = OptionalParameter(str, alt_name="serviceAccountName")
    labels = OptionalParameter(dict, alt_name="labels")
    container_security_context = OptionalParameter(
        dict, alt_name="containerSecurityContext"
    )
    pod_security_context = OptionalParameter(dict, alt_name="podSecurityContext")
    host_affinity = OptionalParameter(dict, alt_name="hostAffinity")
    host_anti_affinity = OptionalParameter(dict, alt_name="hostAntiAffinity")
    node_affinity = OptionalParameter(dict, alt_name="nodeAffinity")
    node_anti_affinity = OptionalParameter(dict, alt_name="nodeAntiAffinity")
    node_selector = OptionalParameter(dict, alt_name="nodeSelector")
    tolerations = OptionalParameter(list, alt_name="tolerations")
    startup_probe = OptionalParameter(dict, alt_name="startupProbe")
    liveness_probe = OptionalParameter(dict, alt_name="livenessProbe")
    readiness_probe = OptionalParameter(dict, alt_name="readinessProbe")
    resources = OptionalParameter(dict, alt_name="resources")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.health_check = HttpHealthCheckPlugin()
        """
        Including health check plugin by default.
        """

        self.image_pull_policy = "Always"
        self.restart_policy = "Never"
        self.env = None
        self.env_from = None
        self.volume_mounts = None
        self.containers = None
        self.volumes = None
        self.termination_grace_period_seconds = 300
        self.service_account_name = "default"
        self.labels = None
        self.container_security_context = {"privileged": True}
        self.pod_security_context = None
        self.host_affinity = None
        self.host_anti_affinity = None
        self.node_affinity = None
        self.node_anti_affinity = None
        self.node_selector = None
        self.tolerations = None
        self.resources = None
        self.liveness_probe = None

        # Startup probe to identify, if the operator has already started.
        # It is necessary to identify unhealthy scenario, if already
        # started, but not ready.
        self.startup_probe = {
            "httpGet": {
                "path": "/check",
                "port": self.health_check.get_parameter("port"),
            },
            "periodSeconds": 1,
            "failureThreshold": 300,
        }

        # Readiness probe to identify unhealthy scenarios. The reason for
        # using it instead of liveness is that it is simpler to detect.
        self.readiness_probe = {
            "httpGet": {
                "path": "/check",
                "port": self.health_check.get_parameter("port"),
            },
            "initialDelaySeconds": 0,
            "periodSeconds": 30,
            "timeoutSeconds": 5,
            "failureThreshold": 20,
        }
