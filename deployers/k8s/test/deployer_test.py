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
import unittest

from kubernetes import config
from kubernetes.client import V1Secret, V1PodList, ApiException, V1Pod, Configuration, V1ObjectMeta, V1Namespace

from pypz.core.commons.utils import convert_to_dict
from pypz.core.specs.misc import BlankOperator
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.deployers.base import DeploymentState
from pypz.deployers.k8s import KubernetesDeployer, DeploymentConflictException, KubernetesParameter
from pypz.executors.commons import ExecutionMode


class TestPipeline(Pipeline):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.operator_a: BlankOperator = BlankOperator()
        self.operator_a.set_parameter("replicationFactor", 1)

        self.operator_b: BlankOperator = BlankOperator()
        self.operator_c: BlankOperator = BlankOperator()
        self.operator_d: BlankOperator = BlankOperator()


class KubernetesDeployerTest(unittest.TestCase):

    test_namespace = "pypz-test"
    test_image = "pypz-test-image"

    config.load_kube_config()
    kubernetes_deployer: KubernetesDeployer = KubernetesDeployer(namespace=test_namespace,
                                                                 configuration=Configuration.get_default_copy())

    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.kubernetes_deployer._core_v1_api.create_namespace(
                body=V1Namespace(metadata=V1ObjectMeta(name=KubernetesDeployerTest.test_namespace)))
        except ApiException as e:
            if 409 == e.status:
                pass
            else:
                raise

    @classmethod
    def tearDownClass(cls) -> None:
        KubernetesDeployerTest.kubernetes_deployer._core_v1_api.delete_namespace(
            KubernetesDeployerTest.test_namespace)

        try:
            while True:
                cls.kubernetes_deployer._core_v1_api.read_namespace(KubernetesDeployerTest.test_namespace)
        except ApiException as e:
            if 404 == e.status:
                pass
            else:
                raise

    def setUp(self) -> None:
        KubernetesDeployerTest.kubernetes_deployer._core_v1_api\
            .delete_collection_namespaced_pod(KubernetesDeployerTest.test_namespace, grace_period_seconds=0)

        KubernetesDeployerTest.kubernetes_deployer._core_v1_api\
            .delete_collection_namespaced_secret(KubernetesDeployerTest.test_namespace, grace_period_seconds=0)

        KubernetesDeployerTest.kubernetes_deployer._core_v1_api\
            .delete_collection_namespaced_config_map(KubernetesDeployerTest.test_namespace, grace_period_seconds=0)

    def test_pod_name_translation(self):
        pipeline = TestPipeline("pipeline")
        self.assertEqual("pipeline-operator-a", KubernetesDeployer.sanitize(pipeline.operator_a.get_full_name()))

    def test_deploy_normal_pipeline_expect_secret_and_pods_deployed(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        secret: V1Secret = client.read_namespaced_secret(pipeline.get_full_name(), KubernetesDeployerTest.test_namespace)

        self.assertIsNotNone(secret)

        pod_list: V1PodList = client.list_namespaced_pod(KubernetesDeployerTest.test_namespace)
        pod_names = {pod.metadata.name for pod in pod_list.items}

        self.assertEqual(5, len(pod_list.items))
        self.assertIn("pipeline-operator-a", pod_names)
        self.assertIn("pipeline-operator-a-0", pod_names)
        self.assertIn("pipeline-operator-b", pod_names)
        self.assertIn("pipeline-operator-c", pod_names)
        self.assertIn("pipeline-operator-d", pod_names)

        for pod_name in pod_names:
            self.assertIn(pod_name, secret.metadata.labels)

    def test_deploy_with_existing_pipeline_deployment_expect_error(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        with self.assertRaises(DeploymentConflictException):
            KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

    def test_deploy_with_existing_operator_deployments_expect_error(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        client.delete_namespaced_secret(pipeline.get_full_name(), KubernetesDeployerTest.test_namespace)

        with self.assertRaises(DeploymentConflictException):
            KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

    def test_deploy_with_error_during_deployment_expect_rollback(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        # Making operator's get_operator_image_name() to raise exception
        setattr(pipeline.operator_d, "get_operator_image_name", lambda: 1 / 0)

        with self.assertRaises(ZeroDivisionError):
            KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        with self.assertRaises(ApiException) as e:
            client.read_namespaced_secret(pipeline.get_full_name(), KubernetesDeployerTest.test_namespace)

        self.assertEqual(404, e.exception.status)

        self.assertEqual(0, len(client.list_namespaced_pod(KubernetesDeployerTest.test_namespace).items))

    def test_destroy_expect_deleted_secret_and_pods(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        KubernetesDeployerTest.kubernetes_deployer.destroy(pipeline.get_full_name())

        with self.assertRaises(ApiException) as e:
            client.read_namespaced_secret(pipeline.get_full_name(), KubernetesDeployerTest.test_namespace)

        self.assertEqual(404, e.exception.status)

        self.assertEqual(0, len(client.list_namespaced_pod(KubernetesDeployerTest.test_namespace).items))

    def test_destroy_with_force_mode_expect_deleted_secret_and_pods(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)
        KubernetesDeployerTest.kubernetes_deployer.destroy(pipeline.get_full_name(), force=True)

        with self.assertRaises(ApiException) as e:
            client.read_namespaced_secret(pipeline.get_full_name(), KubernetesDeployerTest.test_namespace)

        self.assertEqual(404, e.exception.status)

        self.assertEqual(0, len(client.list_namespaced_pod(KubernetesDeployerTest.test_namespace).items))

    def test_destroy_operator_expect_operator_to_be_deleted(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)
        KubernetesDeployerTest.kubernetes_deployer.destroy_operator(pipeline.operator_a.get_full_name())

        with self.assertRaises(ApiException) as e:
            client.read_namespaced_pod("pipeline-operator-a", KubernetesDeployerTest.test_namespace)

        self.assertEqual(404, e.exception.status)

    def test_restart_destroyed_operator_expect_operator_pod_to_be_deployed(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)
        KubernetesDeployerTest.kubernetes_deployer.destroy_operator(pipeline.operator_a.get_full_name())
        time.sleep(1)
        KubernetesDeployerTest.kubernetes_deployer.restart_operator(pipeline.operator_a.get_full_name())

        self.assertIsNotNone(client.read_namespaced_pod("pipeline-operator-a",
                                                        KubernetesDeployerTest.test_namespace))

    def test_restart_running_operator_expect_operator_pod_to_be_destroyed_and_deployed(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        pod: V1Pod = client.read_namespaced_pod("pipeline-operator-a", KubernetesDeployerTest.test_namespace)
        time.sleep(1)
        KubernetesDeployerTest.kubernetes_deployer.restart_operator(pipeline.operator_a.get_full_name())

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        restarted_pod: V1Pod = client.read_namespaced_pod("pipeline-operator-a",
                                                          KubernetesDeployerTest.test_namespace)

        self.assertTrue(1 < (restarted_pod.status.start_time.timestamp() - pod.status.start_time.timestamp()))

    def test_is_deployed(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        self.assertTrue(KubernetesDeployerTest.kubernetes_deployer.is_deployed(pipeline.get_full_name()))

    def test_retrieve_pipeline_deployments(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        self.assertEqual({"pipeline"}, KubernetesDeployerTest.kubernetes_deployer.retrieve_pipeline_deployments())

    def test_retrieve_deployed_pipeline(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        deployed_pipeline = \
            KubernetesDeployerTest.kubernetes_deployer.retrieve_deployed_pipeline(pipeline.get_full_name())

        self.assertEqual(pipeline, deployed_pipeline)

    def test_retrieve_operator_state_all_normal(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        for operator in pipeline.get_protected().get_nested_instances().values():
            self.assertEqual(DeploymentState.Completed,
                             KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_state(operator.get_full_name()))

    def test_retrieve_pipeline_state_all_normal(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        operator_states = KubernetesDeployerTest.kubernetes_deployer.retrieve_pipeline_state(pipeline.get_full_name())

        for operator in pipeline.get_protected().get_nested_instances().values():
            self.assertIn(operator.get_full_name(), operator_states)
            self.assertEqual(DeploymentState.Completed, operator_states[operator.get_full_name()])

    def test_retrieve_operator_state_one_error(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)
        pipeline.operator_a.set_parameter("kubernetes", convert_to_dict(KubernetesParameter(
            imagePullPolicy="Never",
            env=[
                {
                    "name": "PYPZ_TEST_RAISE_ERROR",
                    "value": "true"
                }
            ])))

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while KubernetesDeployerTest.kubernetes_deployer.is_any_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Open, DeploymentState.Running, DeploymentState.NotExisting):
            time.sleep(1)

        self.assertEqual(DeploymentState.Failed,
                         KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_state(pipeline.operator_a.get_full_name()))
        self.assertEqual(DeploymentState.Failed,
                         KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_state(pipeline.operator_a_0.get_full_name()))
        self.assertEqual(DeploymentState.Completed,
                         KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_state(pipeline.operator_b.get_full_name()))
        self.assertEqual(DeploymentState.Completed,
                         KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_state(pipeline.operator_c.get_full_name()))
        self.assertEqual(DeploymentState.Completed,
                         KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_state(pipeline.operator_d.get_full_name()))

    def test_retrieve_operator_logs(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)
        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        operator_logs = \
            KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_logs(pipeline.operator_a.get_full_name())

        self.assertIsNotNone(operator_logs)
        self.assertIn(f"{KubernetesDeployer._env_var_operator_exec_mode}={ExecutionMode.Standard.value}",
                      operator_logs)
        self.assertIn(f"{KubernetesDeployer._env_var_operator_name}={pipeline.operator_a.get_simple_name()}",
                      operator_logs)

    def test_attach_with_one_operator_failing(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)
        pipeline.operator_a.set_parameter("kubernetes", convert_to_dict(KubernetesParameter(
            imagePullPolicy="Never", 
            env=[
                {
                    "name": "PYPZ_TEST_RAISE_ERROR",
                    "value": "true"
                }
            ])))

        def on_operator_failure(operator: Operator, state: DeploymentState):
            if DeploymentState.Failed == state:
                self.assertTrue((operator.get_full_name() == pipeline.operator_a.get_full_name()) or
                                (operator.get_full_name() == pipeline.operator_a_0.get_full_name()))

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)
        KubernetesDeployerTest.kubernetes_deployer.attach(pipeline.get_full_name(),
                                                          on_operator_state_change=on_operator_failure)

    def test_deploy_with_kubernetes_parameters_from_class(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        kubernetes_parameters: KubernetesParameter = KubernetesParameter()

        pipeline.operator_a.set_parameter("kubernetes", convert_to_dict(kubernetes_parameters))

        try:
            KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)
        except:
            self.fail()

    def test_deploy_with_kubernetes_parameters_from_dict(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        pipeline.operator_a.set_parameter("kubernetes", {})

        try:
            KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)
        except:
            self.fail()

    def test_deploy_with_kubernetes_parameters_with_invalid_type(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        pipeline.operator_a.set_parameter("kubernetes", "string")

        with self.assertRaises(TypeError):
            KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

    def test_deploy_with_kubernetes_parameters_with_env_var(self):
        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        kubernetes_parameters: KubernetesParameter = KubernetesParameter()
        kubernetes_parameters.imagePullPolicy = "Never"
        kubernetes_parameters.env = [
            {
                "name": "PYPZ_TEST_ENV_VAR",
                "value": "VAL"
            }
        ]

        pipeline.operator_a.set_parameter("kubernetes", convert_to_dict(kubernetes_parameters))

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        operator_logs = \
            KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_logs(pipeline.operator_a.get_full_name())

        self.assertIn(f"PYPZ_TEST_ENV_VAR=VAL",
                      operator_logs)

    def test_deploy_with_kubernetes_parameters_with_env_var_from_config_map(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api

        config_map = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "test-config-map"
            },
            "data": {
                "PYPZ_TEST_ENV_VAR": "VAL"
            }
        }

        client.create_namespaced_config_map(KubernetesDeployerTest.test_namespace, body=config_map)

        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        kubernetes_parameters: KubernetesParameter = KubernetesParameter()
        kubernetes_parameters.imagePullPolicy = "Never"
        kubernetes_parameters.env = [
            {
                "name": "PYPZ_TEST_ENV_VAR",
                "valueFrom": {
                    "configMapKeyRef": {
                        "name": "test-config-map",
                        "key": "PYPZ_TEST_ENV_VAR"
                    }
                }
            }
        ]

        pipeline.operator_a.set_parameter("kubernetes", convert_to_dict(kubernetes_parameters))

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        operator_logs = \
            KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_logs(pipeline.operator_a.get_full_name())

        self.assertIn(f"PYPZ_TEST_ENV_VAR=VAL",
                      operator_logs)

    def test_deploy_with_kubernetes_parameters_with_env_var_from_secret(self):
        client = KubernetesDeployerTest.kubernetes_deployer._core_v1_api

        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "test-secret"
            },
            "stringData": {
                "PYPZ_TEST_ENV_VAR": "VAL"
            }
        }

        client.create_namespaced_secret(KubernetesDeployerTest.test_namespace, body=secret)

        pipeline = TestPipeline("pipeline")
        pipeline.set_parameter(">kubernetes", convert_to_dict(KubernetesParameter(imagePullPolicy="Never")))
        pipeline.set_parameter(">operatorImageName", KubernetesDeployerTest.test_image)

        kubernetes_parameters: KubernetesParameter = KubernetesParameter()
        kubernetes_parameters.imagePullPolicy = "Never"
        kubernetes_parameters.env = [
            {
                "name": "PYPZ_TEST_ENV_VAR",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "test-secret",
                        "key": "PYPZ_TEST_ENV_VAR"
                    }
                }
            }
        ]

        pipeline.operator_a.set_parameter("kubernetes", convert_to_dict(kubernetes_parameters))

        KubernetesDeployerTest.kubernetes_deployer.deploy(pipeline)

        while not KubernetesDeployerTest.kubernetes_deployer.is_all_operator_in_state(pipeline.get_full_name(),
                                                                                      DeploymentState.Completed):
            time.sleep(1)

        operator_logs = \
            KubernetesDeployerTest.kubernetes_deployer.retrieve_operator_logs(pipeline.operator_a.get_full_name())

        self.assertIn(f"PYPZ_TEST_ENV_VAR=VAL",
                      operator_logs)