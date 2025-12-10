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

from kubernetes.client import Configuration, ApiClient, CoreV1Api, V1SecretList, V1Secret, V1ObjectMeta, V1Pod, \
    ApiException
from kubernetes import config
import certifi

from pypz.core.commons.loggers import DefaultContextLogger, ContextLogger
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.deployers.base import Deployer, DeploymentState
from pypz.executors.commons import ExecutionMode


class DeploymentConflictException(Exception):
    pass


class DeploymentNotFoundException(Exception):
    pass


class DeploymentException(Exception):
    pass


class KubernetesParameter:
    """
    This class represents all Pod fields that can be updated via instance parameters
    """

    def __init__(self,
                 imagePullPolicy: Optional[str] = None,
                 restartPolicy: Optional[str] = None,
                 env: Optional[list[dict]] = None,
                 envFrom: Optional[list[dict]] = None,
                 volumeMounts: Optional[list[dict]] = None,
                 containers: Optional[list[dict]] = None,
                 volumes: Optional[list[dict]] = None,
                 terminationGracePeriodSeconds: Optional[int] = None,
                 serviceAccountName: Optional[str] = None,
                 labels: Optional[dict] = None,
                 containerSecurityContext: Optional[dict] = None,
                 podSecurityContext: Optional[dict] = None,
                 hostAffinity: Optional[dict] = None,
                 hostAntiAffinity: Optional[dict] = None,
                 nodeAffinity: Optional[dict] = None,
                 nodeSelector: Optional[dict] = None,
                 tolerations: Optional[list] = None,
                 nodeAntiAffinity: Optional[dict] = None,
                 startupProbe: Optional[dict] = None,
                 livenessProbe: Optional[dict] = None,
                 readinessProbe: Optional[dict] = None):
        self.imagePullPolicy: Optional[str] = imagePullPolicy
        self.restartPolicy: Optional[str] = restartPolicy
        self.env: Optional[list[dict]] = env
        self.envFrom: Optional[list[dict]] = envFrom
        self.volumeMounts: Optional[list[dict]] = volumeMounts
        self.containers: Optional[list[dict]] = containers
        self.volumes: Optional[list[dict]] = volumes
        self.terminationGracePeriodSeconds: Optional[int] = terminationGracePeriodSeconds
        self.serviceAccountName: Optional[str] = serviceAccountName
        self.labels: Optional[dict] = labels
        self.containerSecurityContext: Optional[dict] = containerSecurityContext
        self.podSecurityContext: Optional[dict] = podSecurityContext
        self.hostAffinity: Optional[dict] = hostAffinity
        self.hostAntiAffinity: Optional[dict] = hostAntiAffinity
        self.nodeSelector: Optional[dict] = nodeSelector
        self.tolerations: Optional[list] = tolerations
        self.nodeAffinity: Optional[dict] = nodeAffinity
        self.nodeAntiAffinity: Optional[dict] = nodeAntiAffinity
        self.startupProbe: Optional[dict] = startupProbe
        self.livenessProbe: Optional[dict] = livenessProbe
        self.readinessProbe: Optional[dict] = readinessProbe


class KubernetesDeployer(Deployer):
    # ========================= class variables ==========================

    _label_key_instance_type = "pypz.io/instance-type"
    _label_key_exec_mode = "pypz.io/exec-mode"
    _label_key_part_of = "pypz.io/part-of"
    _label_key_instance_name = "pypz.io/instance-name"
    _label_value_pipeline = "pipeline"
    _label_value_operator = "operator"

    _pipeline_config_secret_key = "pipeline-config"

    _env_var_operator_name = "PYPZ_OPERATOR_INSTANCE_NAME"
    _env_var_operator_exec_mode = "PYPZ_OPERATOR_EXEC_MODE"

    # ========================= static methods ==========================

    @staticmethod
    def sanitize(string: str) -> str:
        return string.translate(str.maketrans({"_": "-", ".": "-"}))  # type: ignore

    # ========================= ctor ==========================

    def __init__(self,
                 namespace: str = "default",
                 configuration: Configuration = None,
                 config_file: Any = None,
                 verify_ssl: bool = True):
        if configuration is None:
            config.load_kube_config(config_file=config_file)
            configuration = Configuration.get_default_copy()
            configuration.ssl_ca_cert = certifi.where()

        configuration.verify_ssl = verify_ssl

        self._core_v1_api: CoreV1Api = CoreV1Api(api_client=ApiClient(configuration=configuration))

        self._namespace: str = namespace

        self._logger: ContextLogger = \
            ContextLogger(DefaultContextLogger(KubernetesDeployer.__name__))
        self._logger.set_log_level("DEBUG")

    # ========================= implemented methods ==========================

    def deploy(self, pipeline: Pipeline,
               execution_mode: ExecutionMode = ExecutionMode.Standard,
               ignore_operators: list[Operator] = None,
               wait: bool = True) -> None:
        # Check if pipeline is deployed or operators are lingering
        # ========================================================

        if self.is_deployed(pipeline.get_full_name()):
            raise DeploymentConflictException(f"Pipeline already deployed: {pipeline.get_full_name()}")

        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_state = self.retrieve_operator_state(operator.get_full_name())
            if DeploymentState.NotExisting != operator_state:
                raise DeploymentConflictException(f"Operator is still in active state: "
                                                  f"{operator.get_full_name()} - {operator_state}")

        # Deployment
        # ==========

        try:
            # Storing all operators' pod name and simple name, since it can be necessary in some
            # cases like operator restart.
            secret_labels = {
                KubernetesDeployer.sanitize(operator.get_full_name()): operator.get_simple_name()
                for operator in pipeline.get_protected().get_nested_instances().values()
            }
            secret_labels[KubernetesDeployer._label_key_instance_type] = KubernetesDeployer._label_value_pipeline
            secret_labels[KubernetesDeployer._label_key_exec_mode] = execution_mode.value

            # Deploy configuration secret
            secret: V1Secret = V1Secret(
                api_version="v1",
                kind="Secret",
                metadata=V1ObjectMeta(
                    namespace=self._namespace,
                    name=pipeline.get_full_name(),
                    labels=secret_labels
                ),
                data={
                    KubernetesDeployer._pipeline_config_secret_key: base64.b64encode(
                        pipeline.__str__().encode()).decode()
                },
                type="Opaque",
                immutable=True
            )

            self._core_v1_api.create_namespaced_secret(self._namespace, secret)

            # Configuration must be already there before Operator deployment
            while self._retrieve_config_secret(pipeline.get_full_name()) is None:
                time.sleep(1)

            # Operator deployment
            for operator in pipeline.get_protected().get_nested_instances().values():
                self._core_v1_api.create_namespaced_pod(
                    self._namespace,
                    body=self._generate_pod_manifest(
                        operator,
                        execution_mode if (ignore_operators is None) or (operator not in ignore_operators) else
                        ExecutionMode.Skip
                    )
                )

            while wait and self.is_any_operator_in_state(pipeline.get_full_name(), DeploymentState.NotExisting):
                time.sleep(1)

        except Exception as e:
            if isinstance(e, ApiException):
                self._logger.error(f"Status: {e.status}; Reason: {e.reason}")

            self._logger.error("Rolling back deployment ...")

            # Delete deployed operators gracefully
            for operator in pipeline.get_protected().get_nested_instances().values():
                if DeploymentState.NotExisting != self.retrieve_operator_state(operator.get_full_name()):
                    self.destroy_operator(operator.get_full_name(), force=False, wait=False)

            deployed_pods = self._retrieve_operator_pods(pipeline.get_full_name())

            while wait and (0 < len(deployed_pods)):
                self._logger.debug(f"Waiting for operators to be destroyed: {len(deployed_pods)}")
                time.sleep(1)
                deployed_pods = self._retrieve_operator_pods(pipeline.get_full_name())

            # Delete deployed secret
            if self._retrieve_config_secret(pipeline.get_full_name()) is not None:
                self._core_v1_api.delete_namespaced_secret(pipeline.get_full_name(), self._namespace)

                while wait and (self._retrieve_config_secret(pipeline.get_full_name()) is not None):
                    time.sleep(1)

            raise

    def destroy(self, pipeline_name: str, force: bool = False, wait: bool = True) -> None:
        deployed_pipeline = self.retrieve_deployed_pipeline(pipeline_name)
        # Delete deployed operators
        for operator in deployed_pipeline.get_protected().get_nested_instances().values():
            if DeploymentState.NotExisting != self.retrieve_operator_state(operator.get_full_name()):
                self.destroy_operator(operator.get_full_name(), force, wait=False)

        deployed_pods = self._retrieve_operator_pods(pipeline_name)

        while wait and (0 < len(deployed_pods)):
            self._logger.debug(f"Waiting for operators to be destroyed: {len(deployed_pods)}")
            time.sleep(1)
            deployed_pods = self._retrieve_operator_pods(pipeline_name)

        # Delete deployed secret
        if self._retrieve_config_secret(deployed_pipeline.get_full_name()) is not None:
            self._core_v1_api.delete_namespaced_secret(deployed_pipeline.get_full_name(), self._namespace)

            while wait and (self._retrieve_config_secret(deployed_pipeline.get_full_name()) is not None):
                time.sleep(1)

    def restart_operator(self, operator_full_name: str, force: bool = False, wait: bool = True):
        operator_pod_name = KubernetesDeployer.sanitize(operator_full_name)

        secret_list: V1SecretList = self._core_v1_api.list_namespaced_secret(self._namespace,
                                                                             label_selector=operator_pod_name)

        if 0 == len(secret_list.items):
            raise DeploymentException(f"No pipeline configuration found for operator: {operator_full_name}")

        if 1 < len(secret_list.items):
            raise DeploymentException(f"Multiple pipeline configurations found for operator: {operator_full_name}")

        secret: V1Secret = secret_list.items[0]
        operator_simple_name: str = secret.metadata.labels[operator_pod_name]
        exec_mode: ExecutionMode = ExecutionMode(secret.metadata.labels[KubernetesDeployer._label_key_exec_mode])
        deployed_pipeline: Pipeline = self._retrieve_deployed_pipeline_from_secret(secret)
        operator: Operator = deployed_pipeline.get_protected().get_nested_instance(operator_simple_name)

        if self._retrieve_operator_pod(operator_pod_name) is not None:
            self.destroy_operator(operator_pod_name, force)

        self._core_v1_api.create_namespaced_pod(self._namespace,
                                                body=self._generate_pod_manifest(operator, exec_mode))

    def destroy_operator(self, operator_full_name: str, force: bool = False, wait: bool = True) -> None:
        if force:
            self._core_v1_api.delete_namespaced_pod(name=KubernetesDeployer.sanitize(operator_full_name),
                                                    namespace=self._namespace, grace_period_seconds=0)
        else:
            self._core_v1_api.delete_namespaced_pod(name=KubernetesDeployer.sanitize(operator_full_name),
                                                    namespace=self._namespace)

        while wait and (self._retrieve_operator_pod(operator_full_name) is not None):
            time.sleep(1)

    def is_deployed(self, pipeline_name: str) -> bool:
        return self._retrieve_config_secret(pipeline_name) is not None

    def retrieve_pipeline_deployments(self) -> set[str]:
        secret_list: V1SecretList = self._core_v1_api.list_namespaced_secret(
            self._namespace,
            label_selector=f"{KubernetesDeployer._label_key_instance_type}={KubernetesDeployer._label_value_pipeline}")

        return {secret.metadata.name for secret in secret_list.items}

    def retrieve_deployed_pipeline(self, pipeline_name: str) -> Optional[Pipeline]:
        return self._retrieve_deployed_pipeline_from_secret(self._retrieve_config_secret(pipeline_name))

    def retrieve_operator_state(self, operator_full_name: str) -> DeploymentState:
        pod = self._retrieve_operator_pod(operator_full_name)

        if pod is None:
            return DeploymentState.NotExisting

        if (pod.status is None) or (pod.status.phase is None) or (pod.status.container_statuses is None):
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
        # of the process like D state on unmounting etc. For this reason, we need
        # to check, if the container has already been started and is ready. Note
        # that we use readiness probe instead of liveness, since it is easier to
        # catch.

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
            if ready_probe_set and start_probe_set and container_started and not container_ready:
                return DeploymentState.Unhealthy
            return DeploymentState.Running

        return DeploymentState.Unknown

    def retrieve_operator_logs(self, operator_full_name: str, **kwargs) -> Optional[str]:
        try:
            return self._core_v1_api.read_namespaced_pod_log(name=KubernetesDeployer.sanitize(operator_full_name),
                                                             namespace=self._namespace,
                                                             **kwargs)
        except ApiException as e:
            if 404 == e.status:
                return None
            raise

    # ========================= helper methods ==========================

    def _retrieve_config_secret(self, pipeline_name: str) -> Optional[V1Secret]:
        try:
            return self._core_v1_api.read_namespaced_secret(pipeline_name, self._namespace)
        except ApiException as e:
            if 404 == e.status:
                return None
            raise

    def _retrieve_deployed_pipeline_from_secret(self, secret: V1Secret) -> Optional[Pipeline]:
        if KubernetesDeployer._pipeline_config_secret_key not in secret.data:
            raise DeploymentException("Provided secret is not pipeline configuration secret")

        return Pipeline.create_from_string(base64.b64decode(
            secret.data[KubernetesDeployer._pipeline_config_secret_key]))

    def _retrieve_operator_pod(self, operator_full_name: str) -> Optional[V1Pod]:
        try:
            return self._core_v1_api.read_namespaced_pod(KubernetesDeployer.sanitize(operator_full_name),
                                                         self._namespace)
        except ApiException as e:
            if 404 == e.status:
                return None
            raise

    def _retrieve_operator_pods(self, pipeline_name: str) -> list[V1Pod]:
        return self._core_v1_api.list_namespaced_pod(
            self._namespace,
            label_selector=f"{KubernetesDeployer._label_key_part_of}={pipeline_name}"
        ).items

    def _generate_pod_manifest(self, operator: Operator, execution_mode: ExecutionMode) -> dict:
        if operator.get_operator_image_name() is None:
            raise DeploymentException(f"[{operator.get_full_name()}] Image name must be specified")

        operator_pod_name = KubernetesDeployer.sanitize(operator.get_full_name())

        if operator.has_parameter("kubernetes"):
            parameters = operator.get_parameter("kubernetes")
            if isinstance(parameters, dict):
                kubernetes_parameters: KubernetesParameter = KubernetesParameter(**parameters)
            elif isinstance(parameters, KubernetesParameter):
                kubernetes_parameters: KubernetesParameter = parameters
            else:
                raise TypeError(f"Invalid kubernetes parameter type for '{operator.get_full_name()}': "
                                f"{type(parameters)}")
        else:
            kubernetes_parameters: KubernetesParameter = KubernetesParameter()

        env = [
            {
                'name': "PYPZ_NODE_NAME",
                'valueFrom': {
                    "fieldRef": {
                        "fieldPath": "spec.nodeName"
                    }
                },
            },
            {
                'name': KubernetesDeployer._env_var_operator_name,
                'value': operator.get_simple_name(),
            },
            {
                'name': KubernetesDeployer._env_var_operator_exec_mode,
                'value': execution_mode.value
            }
        ]

        if kubernetes_parameters.env is not None:
            env.extend(kubernetes_parameters.env)

        volume_mounts = [
            {
                'mountPath': '/operator/config',
                'name': operator_pod_name
            }
        ]

        if kubernetes_parameters.volumeMounts is not None:
            volume_mounts.extend(kubernetes_parameters.volumeMounts)

        security_context = {
            'privileged': True
        } if kubernetes_parameters.containerSecurityContext is None else kubernetes_parameters.containerSecurityContext

        containers = [
            {
                'env': env,
                'envFrom': kubernetes_parameters.envFrom,
                'image': operator.get_operator_image_name(),
                'imagePullPolicy': 'Always'
                if kubernetes_parameters.imagePullPolicy is None else kubernetes_parameters.imagePullPolicy,
                'name': operator_pod_name,
                'volumeMounts': volume_mounts,
                'securityContext': security_context,
                'livenessProbe': kubernetes_parameters.livenessProbe,
                'readinessProbe': kubernetes_parameters.readinessProbe,
                'startupProbe': kubernetes_parameters.startupProbe
            }
        ]

        if kubernetes_parameters.containers is not None:
            containers.extend(kubernetes_parameters.containers)

        volumes = [
            {
                'name': operator_pod_name,
                'secret': {
                    'defaultMode': 493,
                    'items': [
                        {
                            'key': KubernetesDeployer._pipeline_config_secret_key,
                            'mode': 493,
                            'path': 'config.json'
                        }
                    ],
                    'optional': False,
                    'secretName': operator.get_context().get_full_name()
                }
            }
        ]

        if kubernetes_parameters.volumes is not None:
            volumes.extend(kubernetes_parameters.volumes)

        topology_spread_constraints = [
            {
                'labelSelector': {
                    'matchLabels': {
                        KubernetesDeployer._label_key_part_of: operator.get_context().get_full_name()
                    }
                },
                'maxSkew': 1,
                'topologyKey': 'kubernetes.io/hostname',
                'whenUnsatisfiable': 'ScheduleAnyway'
            }
        ]

        spec: dict[str, Any] = {
            'containers': containers,
            'restartPolicy': 'Never'
            if kubernetes_parameters.restartPolicy is None else kubernetes_parameters.restartPolicy,
            'serviceAccountName': 'default' if kubernetes_parameters.serviceAccountName is None else
            kubernetes_parameters.serviceAccountName,
            'terminationGracePeriodSeconds': 300 if kubernetes_parameters.terminationGracePeriodSeconds is None else
            kubernetes_parameters.terminationGracePeriodSeconds,
            'topologySpreadConstraints': topology_spread_constraints,
            'volumes': volumes,
            'securityContext': kubernetes_parameters.podSecurityContext
        }

        if (kubernetes_parameters.hostAffinity is not None) or (kubernetes_parameters.hostAntiAffinity is not None):
            node_selector_terms = []

            if kubernetes_parameters.hostAffinity is not None:
                node_selector_terms.append({
                    'matchExpressions': [
                        {
                            'key': "kubernetes.io/hostname",
                            'operator': 'In',
                            'values': kubernetes_parameters.hostAffinity
                        }
                    ]
                })

            if kubernetes_parameters.hostAntiAffinity is not None:
                node_selector_terms.append({
                    'matchExpressions': [
                        {
                            'key': "kubernetes.io/hostname",
                            'operator': 'NotIn',
                            'values': kubernetes_parameters.hostAntiAffinity
                        }
                    ]
                })

            spec['affinity'] = {
                'nodeAffinity': {
                    'requiredDuringSchedulingIgnoredDuringExecution': {
                        'nodeSelectorTerms': node_selector_terms
                    }
                }
            }

        """ Notice that since host affinity can be defined separately, we need
            to handle the situation, where both nodeAffinity and hostAffinity is set.
            This is not the case for nodeAntiAffinity. """
        if kubernetes_parameters.nodeAffinity is not None:
            if 'affinity' not in spec:
                spec['affinity'] = dict()
            spec['affinity'].update(kubernetes_parameters.nodeAffinity)

        if kubernetes_parameters.nodeAntiAffinity is not None:
            spec['nodeAntiAffinity'] = kubernetes_parameters.nodeAntiAffinity

        if kubernetes_parameters.nodeSelector is not None:
            spec['nodeSelector'] = kubernetes_parameters.nodeSelector

        if kubernetes_parameters.tolerations is not None:
            spec['tolerations'] = kubernetes_parameters.tolerations

        labels = {
            KubernetesDeployer._label_key_instance_type: KubernetesDeployer._label_value_operator,
            KubernetesDeployer._label_key_part_of: operator.get_context().get_full_name(),
            KubernetesDeployer._label_key_instance_name: operator.get_simple_name()
        }

        if kubernetes_parameters.labels is not None:
            # This is how we ensure that basic labels are not getting overwritten
            labels = kubernetes_parameters.labels.update(labels)

        metadata = {
            'labels': labels,
            'name': operator_pod_name,
        }

        return {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': metadata,
            'spec': spec
        }
