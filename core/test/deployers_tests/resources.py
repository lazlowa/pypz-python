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
from typing import Optional

from pypz.core.specs.misc import BlankOperator
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.deployers.base import Deployer, DeploymentState
from pypz.executors.commons import ExecutionMode


class TestDeployer(Deployer):

    def __init__(self):
        super().__init__()
        self.deployed_pipelines: dict[str, Pipeline] = dict()
        self.deployed_operators: dict[str, DeploymentState] = dict()

    def deploy(self, pipeline: Pipeline,
               execution_mode: ExecutionMode = ExecutionMode.Standard,
               ignore_operators: list[Operator] = None,
               wait: bool = True) -> None:
        self.deployed_pipelines[pipeline.get_full_name()] = pipeline

        for operator in pipeline.get_protected().get_nested_instances().values():
            self.deployed_operators[operator.get_full_name()] = DeploymentState.Running

    def destroy(self, pipeline_name: str, force: bool = False, wait: bool = True) -> None:
        pipeline = self.deployed_pipelines[pipeline_name]

        for operator in pipeline.get_protected().get_nested_instances().values():
            del self.deployed_operators[operator.get_full_name()]

        del self.deployed_pipelines[pipeline_name]

    def restart_operator(self, operator_full_name: str, force: bool = False, wait: bool = True):
        pass

    def destroy_operator(self, operator_full_name: str, force: bool = False, wait: bool = True) -> None:
        del self.deployed_operators[operator_full_name]

    def is_deployed(self, pipeline_name: str) -> bool:
        return pipeline_name in self.deployed_pipelines

    def retrieve_pipeline_deployments(self) -> set[str]:
        return set(self.deployed_pipelines.keys())

    def retrieve_deployed_pipeline(self, pipeline_name: str) -> Optional[Pipeline]:
        return self.deployed_pipelines[pipeline_name]

    def retrieve_operator_state(self, operator_full_name: str) -> DeploymentState:
        if operator_full_name in self.deployed_operators:
            return self.deployed_operators[operator_full_name]
        return DeploymentState.NotExisting

    def retrieve_operator_logs(self, operator_full_name: str) -> Optional[str]:
        pass


class TestPipeline(Pipeline):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.operator_a: BlankOperator = BlankOperator()
        self.operator_a.set_parameter("replicationFactor", 1)

        self.operator_b: BlankOperator = BlankOperator()
        self.operator_c: BlankOperator = BlankOperator()
        self.operator_d: BlankOperator = BlankOperator()
