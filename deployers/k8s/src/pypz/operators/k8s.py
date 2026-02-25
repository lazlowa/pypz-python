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
from typing import Optional

from pypz.core.commons.parameters import OptionalParameter
from pypz.core.specs.operator import Operator
from pypz.plugins.misc.health_check import HttpHealthCheckPlugin

# ----------------------------
# Common constants and methods
# ----------------------------

label_key_instance_type = "pypz.io/instance-type"
label_key_exec_mode = "pypz.io/exec-mode"
label_key_part_of = "pypz.io/part-of"
label_key_instance_name = "pypz.io/instance-name"
label_value_pipeline = "pipeline"
label_value_operator = "operator"

pipeline_config_secret_key = "pipeline-config"

env_var_node_name = "PYPZ_NODE_NAME"
env_var_operator_name = "PYPZ_OPERATOR_INSTANCE_NAME"
env_var_operator_exec_mode = "PYPZ_OPERATOR_EXEC_MODE"


def convert_to_pod_name(string: str) -> str:
    return string.translate(str.maketrans({"_": "-", ".": "-"}))  # type: ignore


# -----------------------------------
# Kubernetes operator related classes
# -----------------------------------


class KubernetesOperator(Operator, ABC):
    _manifest_override = OptionalParameter(dict, alt_name="manifestOverride")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.health_check = HttpHealthCheckPlugin()
        """
        Including health check plugin by default.
        """

        self._manifest_override: Optional[dict] = None
        """
        This dictionary contains the manifest data, which has been provided by the user
        """

    def get_pod_manifest(self) -> dict:
        base_pod_manifest = self._get_pod_manifest_base()

        if not self._manifest_override:
            return base_pod_manifest

        # Some minor validation to ensure basic functionality
        # ---------------------------------------------------

        if "metadata" not in self._manifest_override:
            raise ValueError(
                f"[{self.get_full_name()}] Missing metadata in pod manifest"
            )

        if "name" not in self._manifest_override["metadata"]:
            raise ValueError(
                f"[{self.get_full_name()}] Missing pod name in pod manifest"
            )

        if (
            base_pod_manifest["metadata"]["name"]
            != self._manifest_override["metadata"]["name"]
        ):
            raise ValueError(
                f"[{self.get_full_name()}] Invalid pod name: "
                f"{self._manifest_override['metadata']['name']};"
                f"Expected: {base_pod_manifest['metadata']['name']}"
            )

        if "labels" not in self._manifest_override["metadata"]:
            raise ValueError(
                f"[{self.get_full_name()}] Missing pod labels in pod manifest"
            )

        if (
            label_key_instance_type not in self._manifest_override["metadata"]["labels"]
            or self._manifest_override["metadata"]["labels"][label_key_instance_type]
            != base_pod_manifest["metadata"]["labels"][label_key_instance_type]
        ):
            raise ValueError(
                f"[{self.get_full_name()}] Missing or invalid label value for "
                f"label '{label_key_instance_type}', expected: "
                f"{base_pod_manifest['metadata']['labels'][label_key_instance_type]} "
            )

        if (
            label_key_part_of not in self._manifest_override["metadata"]["labels"]
            or self._manifest_override["metadata"]["labels"][label_key_part_of]
            != base_pod_manifest["metadata"]["labels"][label_key_part_of]
        ):
            raise ValueError(
                f"[{self.get_full_name()}] Missing or invalid label value for "
                f"label '{label_key_part_of}', expected: "
                f"{base_pod_manifest['metadata']['labels'][label_key_part_of]} "
            )

        if (
            label_key_instance_name not in self._manifest_override["metadata"]["labels"]
            or self._manifest_override["metadata"]["labels"][label_key_instance_name]
            != base_pod_manifest["metadata"]["labels"][label_key_instance_name]
        ):
            raise ValueError(
                f"[{self.get_full_name()}] Missing or invalid label value for "
                f"label '{label_key_instance_name}', expected: "
                f"{base_pod_manifest['metadata']['labels'][label_key_instance_name]} "
            )

        return self._manifest_override

    def _get_pod_manifest_base(self) -> dict:
        if self.get_operator_image_name() is None:
            raise ValueError(
                f"[{self.get_full_name()}] Operator image name must be specified"
            )

        operator_pod_name = convert_to_pod_name(self.get_full_name())

        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": operator_pod_name,
                "labels": {
                    label_key_instance_type: label_value_operator,
                    label_key_part_of: self.get_context().get_full_name(),
                    label_key_instance_name: self.get_simple_name(),
                },
            },
            "spec": {
                "restartPolicy": "Never",
                "serviceAccountName": "default",
                "terminationGracePeriodSeconds": 300,
                "topologySpreadConstraints": [
                    {
                        "labelSelector": {
                            "matchLabels": {
                                label_key_part_of: self.get_context().get_full_name()
                            }
                        },
                        "maxSkew": 1,
                        "topologyKey": "kubernetes.io/hostname",
                        "whenUnsatisfiable": "ScheduleAnyway",
                    }
                ],
                "volumes": [
                    {
                        "name": operator_pod_name,
                        "secret": {
                            "defaultMode": 493,
                            "items": [
                                {
                                    "key": pipeline_config_secret_key,
                                    "mode": 493,
                                    "path": "config.json",
                                }
                            ],
                            "optional": False,
                            "secretName": self.get_context().get_full_name(),
                        },
                    }
                ],
                "containers": [
                    {
                        "name": operator_pod_name,
                        "image": self.get_operator_image_name(),
                        "imagePullPolicy": "Always",
                        "env": [
                            {
                                "name": env_var_node_name,
                                "valueFrom": {
                                    "fieldRef": {"fieldPath": "spec.nodeName"}
                                },
                            },
                            {
                                "name": env_var_operator_name,
                                "value": self.get_simple_name(),
                            },
                        ],
                        "volumeMounts": [
                            {"mountPath": "/operator/config", "name": operator_pod_name}
                        ],
                        # Readiness probe to identify unhealthy scenarios. The reason for
                        # using it instead of liveness is that it is simpler to detect.
                        "readinessProbe": {
                            "httpGet": {
                                "path": "/check",
                                "port": self.health_check.get_parameter("port"),
                            },
                            "initialDelaySeconds": 0,
                            "periodSeconds": 30,
                            "timeoutSeconds": 5,
                            "failureThreshold": 20,
                        },
                        # Startup probe to identify, if the operator has already started.
                        # It is necessary to identify unhealthy scenario, if already started, but not ready.
                        "startupProbe": {
                            "httpGet": {
                                "path": "/check",
                                "port": self.health_check.get_parameter("port"),
                            },
                            "periodSeconds": 1,
                            "failureThreshold": 300,
                        },
                    }
                ],
            },
        }

        return pod_manifest
