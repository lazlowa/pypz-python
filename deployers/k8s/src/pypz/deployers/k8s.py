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
import base64
import time
from typing import Any, Optional

import certifi
from kubernetes import config
from kubernetes.client import (
    ApiClient,
    ApiException,
    Configuration,
    CoreV1Api,
    V1ObjectMeta,
    V1Pod,
    V1Secret,
    V1SecretList,
)
from pypz.core.commons.loggers import ContextLogger, DefaultContextLogger
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.deployers.base import Deployer, DeploymentState
from pypz.executors.commons import ExecutionMode
from pypz.operators.k8s import (
    KubernetesOperator,
    convert_to_pod_name,
    env_var_operator_exec_mode,
    label_key_exec_mode,
    label_key_instance_type,
    label_key_part_of,
    label_value_pipeline,
    pipeline_config_secret_key,
)


class DeploymentConflictException(Exception):
    pass


class DeploymentNotFoundException(Exception):
    pass


class DeploymentException(Exception):
    pass


class KubernetesDeployer(Deployer):
    # ========================= class variables ==========================

    # ========================= static methods ==========================

    # ========================= ctor ==========================

    def __init__(
        self,
        namespace: str = "default",
        configuration: Configuration = None,
        config_file: Any = None,
        verify_ssl: bool = True,
    ):
        if configuration is None:
            config.load_kube_config(config_file=config_file)
            configuration = Configuration.get_default_copy()
            configuration.ssl_ca_cert = certifi.where()

        configuration.verify_ssl = verify_ssl

        self._core_v1_api: CoreV1Api = CoreV1Api(
            api_client=ApiClient(configuration=configuration)
        )

        self._namespace: str = namespace

        self._logger: ContextLogger = ContextLogger(
            DefaultContextLogger(KubernetesDeployer.__name__)
        )
        self._logger.set_log_level("DEBUG")

    # ========================= implemented methods ==========================

    def deploy(
        self,
        pipeline: Pipeline,
        execution_mode: ExecutionMode = ExecutionMode.Standard,
        ignore_operators: list[Operator] = None,
        wait: bool = True,
    ) -> None:
        # Check for incompatible operator types
        # =====================================

        for operator in pipeline.get_protected().get_nested_instances().values():
            if not isinstance(operator, KubernetesOperator):
                raise TypeError(
                    f"[{operator.get_full_name()}] Invalid operator type, operators must"
                    f"extend on {KubernetesOperator}"
                )

        # Check if pipeline is deployed or operators are lingering
        # ========================================================

        if self.is_deployed(pipeline.get_full_name()):
            raise DeploymentConflictException(
                f"Pipeline already deployed: {pipeline.get_full_name()}"
            )

        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_state = self.retrieve_operator_state(operator.get_full_name())
            if DeploymentState.NotExisting != operator_state:
                raise DeploymentConflictException(
                    f"Operator is still in active state: "
                    f"{operator.get_full_name()} - {operator_state}"
                )

        # Deployment
        # ==========

        try:
            # Storing all operators' pod name and simple name, since it can be necessary in some
            # cases like operator restart.
            secret_labels = {
                convert_to_pod_name(
                    operator.get_full_name()
                ): operator.get_simple_name()
                for operator in pipeline.get_protected().get_nested_instances().values()
            }
            secret_labels[label_key_instance_type] = label_value_pipeline
            secret_labels[label_key_exec_mode] = execution_mode.value

            # Deploy configuration secret
            secret: V1Secret = V1Secret(
                api_version="v1",
                kind="Secret",
                metadata=V1ObjectMeta(
                    namespace=self._namespace,
                    name=pipeline.get_full_name(),
                    labels=secret_labels,
                ),
                data={
                    pipeline_config_secret_key: base64.b64encode(
                        pipeline.__str__().encode()
                    ).decode()
                },
                type="Opaque",
                immutable=True,
            )

            self._core_v1_api.create_namespaced_secret(self._namespace, secret)

            # Configuration must be already there before Operator deployment
            while self._retrieve_config_secret(pipeline.get_full_name()) is None:
                time.sleep(1)

            # Operator deployment
            for operator in (
                pipeline.get_protected().get_nested_instances().values()
            ):  # type: KubernetesOperator
                manifest = operator.get_pod_manifest()

                operator_pod_name = convert_to_pod_name(operator.get_full_name())

                # Extend container's environment variables by the execution mode
                operator_container: Optional[dict] = None
                for container in manifest["spec"]["containers"]:
                    if operator_pod_name == container["name"]:
                        operator_container = container
                        break

                if not operator_container:
                    raise DeploymentException(
                        f"[{operator.get_full_name()}] Pod manifest has no operator container"
                    )

                operator_container["env"].extend(
                    [
                        {
                            "name": env_var_operator_exec_mode,
                            "value": (
                                execution_mode.value
                                if (ignore_operators is None)
                                or (operator not in ignore_operators)
                                else ExecutionMode.Skip.value
                            ),
                        },
                    ]
                )

                self._core_v1_api.create_namespaced_pod(
                    self._namespace,
                    body=manifest,
                )

            while wait and self.is_any_operator_in_state(
                pipeline.get_full_name(), DeploymentState.NotExisting
            ):
                time.sleep(1)

        except Exception as e:
            if isinstance(e, ApiException):
                self._logger.error(f"Status: {e.status}; Reason: {e.reason}")

            self._logger.error("Rolling back deployment ...")

            # Delete deployed operators gracefully
            for operator in pipeline.get_protected().get_nested_instances().values():
                if DeploymentState.NotExisting != self.retrieve_operator_state(
                    operator.get_full_name()
                ):
                    self.destroy_operator(
                        operator.get_full_name(), force=False, wait=False
                    )

            deployed_pods = self._retrieve_operator_pods(pipeline.get_full_name())

            while wait and (0 < len(deployed_pods)):
                self._logger.debug(
                    f"Waiting for operators to be destroyed: {len(deployed_pods)}"
                )
                time.sleep(1)
                deployed_pods = self._retrieve_operator_pods(pipeline.get_full_name())

            # Delete deployed secret
            if self._retrieve_config_secret(pipeline.get_full_name()) is not None:
                self._core_v1_api.delete_namespaced_secret(
                    pipeline.get_full_name(), self._namespace
                )

                while wait and (
                    self._retrieve_config_secret(pipeline.get_full_name()) is not None
                ):
                    time.sleep(1)

            raise

    def destroy(
        self, pipeline_name: str, force: bool = False, wait: bool = True
    ) -> None:
        deployed_pipeline = self.retrieve_deployed_pipeline(pipeline_name)
        # Delete deployed operators
        for operator in (
            deployed_pipeline.get_protected().get_nested_instances().values()
        ):
            if DeploymentState.NotExisting != self.retrieve_operator_state(
                operator.get_full_name()
            ):
                self.destroy_operator(operator.get_full_name(), force, wait=False)

        deployed_pods = self._retrieve_operator_pods(pipeline_name)

        while wait and (0 < len(deployed_pods)):
            self._logger.debug(
                f"Waiting for operators to be destroyed: {len(deployed_pods)}"
            )
            time.sleep(1)
            deployed_pods = self._retrieve_operator_pods(pipeline_name)

        # Delete deployed secret
        if self._retrieve_config_secret(deployed_pipeline.get_full_name()) is not None:
            self._core_v1_api.delete_namespaced_secret(
                deployed_pipeline.get_full_name(), self._namespace
            )

            while wait and (
                self._retrieve_config_secret(deployed_pipeline.get_full_name())
                is not None
            ):
                time.sleep(1)

    def restart_operator(
        self, operator_full_name: str, force: bool = False, wait: bool = True
    ):
        operator_pod_name = KubernetesOperator.sanitize(operator_full_name)

        secret_list: V1SecretList = self._core_v1_api.list_namespaced_secret(
            self._namespace, label_selector=operator_pod_name
        )

        if 0 == len(secret_list.items):
            raise DeploymentException(
                f"No pipeline configuration found for operator: {operator_full_name}"
            )

        if 1 < len(secret_list.items):
            raise DeploymentException(
                f"Multiple pipeline configurations found for operator: {operator_full_name}"
            )

        secret: V1Secret = secret_list.items[0]
        operator_simple_name: str = secret.metadata.labels[operator_pod_name]
        exec_mode: ExecutionMode = ExecutionMode(
            secret.metadata.labels[label_key_exec_mode]
        )
        deployed_pipeline: Pipeline = self._retrieve_deployed_pipeline_from_secret(
            secret
        )
        operator: Operator = deployed_pipeline.get_protected().get_nested_instance(
            operator_simple_name
        )

        if self._retrieve_operator_pod(operator_pod_name) is not None:
            self.destroy_operator(operator_pod_name, force)

        self._core_v1_api.create_namespaced_pod(
            self._namespace, body=self._generate_pod_manifest(operator, exec_mode)
        )

    def destroy_operator(
        self, operator_full_name: str, force: bool = False, wait: bool = True
    ) -> None:
        if force:
            self._core_v1_api.delete_namespaced_pod(
                name=KubernetesOperator.sanitize(operator_full_name),
                namespace=self._namespace,
                grace_period_seconds=0,
            )
        else:
            self._core_v1_api.delete_namespaced_pod(
                name=KubernetesOperator.sanitize(operator_full_name),
                namespace=self._namespace,
            )

        while wait and (self._retrieve_operator_pod(operator_full_name) is not None):
            time.sleep(1)

    def is_deployed(self, pipeline_name: str) -> bool:
        return self._retrieve_config_secret(pipeline_name) is not None

    def retrieve_pipeline_deployments(self) -> set[str]:
        secret_list: V1SecretList = self._core_v1_api.list_namespaced_secret(
            self._namespace,
            label_selector=f"{label_key_instance_type}={label_value_pipeline}",
        )

        return {secret.metadata.name for secret in secret_list.items}

    def retrieve_deployed_pipeline(self, pipeline_name: str) -> Optional[Pipeline]:
        return self._retrieve_deployed_pipeline_from_secret(
            self._retrieve_config_secret(pipeline_name)
        )

    def retrieve_operator_state(self, operator_full_name: str) -> DeploymentState:
        pod = self._retrieve_operator_pod(operator_full_name)

        if pod is None:
            return DeploymentState.NotExisting

        # Theoretically, once the pod is initialized, these values shall
        # never be None again, however in some rare circumstances, it can
        # be None after initialized.
        # Notice that at this point, we only check the values necessary
        # for the next evaluations.
        if (pod.status is None) or (pod.status.phase is None):
            return DeploymentState.Unknown

        # Interpreting the statuses
        # -------------------------
        # Notice that if the pod's phase is not running, we don't care about the
        # readiness and startup, since we have enough information. However, if
        # there are multiple containers in the Pod (e.g., Istio), then we need
        # to check the main container's state below.

        if "Succeeded" == pod.status.phase:
            return DeploymentState.Completed

        if "Failed" == pod.status.phase:
            return DeploymentState.Failed

        if "Pending" == pod.status.phase:
            return DeploymentState.Open

        # If the pod is in Running state:
        # We cannot rely only on statuses as we may miss runtime issues outside
        # the process like D state on unmounting etc. For this reason, we need
        # to check, if the container has already been started and is ready. Note
        # that we use readiness probe instead of liveness, since it is easier to
        # catch.

        # Explanation see above at pod/status None check. Note that the checks
        # are split, since the container status check is only necessary in case,
        # where the pod is already in Running state. By this we can avoid returning
        # Unknown state, if pod would be already in pending, but containers did
        # not start yet.
        if pod.status.container_statuses is None:
            return DeploymentState.Unknown

        # Check, if the necessary probes are set, since if not, we need to fall
        # back to check the pod state.
        ready_probe_set = False
        start_probe_set = False
        for container_spec in pod.spec.containers:
            if container_spec.name == pod.metadata.name:
                ready_probe_set = container_spec.readiness_probe is not None
                start_probe_set = container_spec.startup_probe is not None
                break

        container_started = False
        container_ready = False
        for container_status in pod.status.container_statuses:
            if container_status.name == pod.metadata.name:
                container_started = container_status.started
                container_ready = container_status.ready
                break

        if "Running" == pod.status.phase:
            # We can identify unhealthy situation only, if the startup and
            # readiness probes are set, otherwise fall back to pod state.
            if (
                ready_probe_set
                and start_probe_set
                and container_started
                and not container_ready
            ):
                return DeploymentState.Unhealthy
            return DeploymentState.Running

        return DeploymentState.Unknown

    def retrieve_operator_logs(
        self, operator_full_name: str, **kwargs
    ) -> Optional[str]:
        try:
            return self._core_v1_api.read_namespaced_pod_log(
                name=KubernetesOperator.sanitize(operator_full_name),
                namespace=self._namespace,
                **kwargs,
            )
        except ApiException as e:
            if 404 == e.status:
                return None
            raise

    # ========================= helper methods ==========================

    def _retrieve_config_secret(self, pipeline_name: str) -> Optional[V1Secret]:
        try:
            return self._core_v1_api.read_namespaced_secret(
                pipeline_name, self._namespace
            )
        except ApiException as e:
            if 404 == e.status:
                return None
            raise

    def _retrieve_deployed_pipeline_from_secret(
        self, secret: V1Secret
    ) -> Optional[Pipeline]:
        if pipeline_config_secret_key not in secret.data:
            raise DeploymentException(
                "Provided secret is not pipeline configuration secret"
            )

        return Pipeline.create_from_string(
            base64.b64decode(secret.data[pipeline_config_secret_key])
        )

    def _retrieve_operator_pod(self, operator_full_name: str) -> Optional[V1Pod]:
        try:
            return self._core_v1_api.read_namespaced_pod(
                KubernetesOperator.sanitize(operator_full_name), self._namespace
            )
        except ApiException as e:
            if 404 == e.status:
                return None
            raise

    def _retrieve_operator_pods(self, pipeline_name: str) -> list[V1Pod]:
        return self._core_v1_api.list_namespaced_pod(
            self._namespace,
            label_selector=f"{label_key_part_of}={pipeline_name}",
        ).items
