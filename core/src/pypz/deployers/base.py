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
import enum
import time
from abc import ABC, abstractmethod
from typing import Callable, Optional

from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.executors.commons import ExecutionMode


class DeploymentState(enum.Enum):
    Open = "Open",
    Running = "Running",
    Completed = "Completed",
    Failed = "Failed",
    Unknown = "Unknown",
    NotExisting = "NotExisting"


class Deployer(ABC):

    # ================== abstract methods ====================

    @abstractmethod
    def deploy(self, pipeline: Pipeline,
               execution_mode: ExecutionMode = ExecutionMode.Standard,
               ignore_operators: list[Operator] = None,
               wait: bool = True) -> None:
        pass

    @abstractmethod
    def destroy(self, pipeline_name: str, force: bool = False, wait: bool = True) -> None:
        pass

    @abstractmethod
    def restart_operator(self, operator_full_name: str, force: bool = False, wait: bool = True) -> None:
        pass

    @abstractmethod
    def destroy_operator(self, operator_full_name: str, force: bool = False, wait: bool = True) -> None:
        pass

    @abstractmethod
    def is_deployed(self, pipeline_name: str) -> bool:
        pass

    @abstractmethod
    def retrieve_pipeline_deployments(self) -> set[str]:
        pass

    @abstractmethod
    def retrieve_deployed_pipeline(self, pipeline_name: str) -> Optional[Pipeline]:
        pass

    @abstractmethod
    def retrieve_operator_state(self, operator_full_name: str) -> DeploymentState:
        pass

    @abstractmethod
    def retrieve_operator_logs(self, operator_full_name: str) -> Optional[str]:
        pass

    # ================== public methods ====================

    def retrieve_pipeline_state(self, pipeline_name: str) -> dict[str, DeploymentState]:
        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)

        operator_states = dict()
        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_states[operator.get_full_name()] = self.retrieve_operator_state(operator.get_full_name())

        return operator_states

    def is_any_operator_in_state(self, pipeline_name: str, *state: DeploymentState):
        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)
        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_state = self.retrieve_operator_state(operator.get_full_name())
            if operator_state in state:
                return True
        return False

    def is_all_operator_in_state(self, pipeline_name: str, *state: DeploymentState):
        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)
        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_state = self.retrieve_operator_state(operator.get_full_name())
            if operator_state not in state:
                return False
        return True

    def attach(self, pipeline_name: str,
               on_operator_state_change: Callable[[Operator, DeploymentState], None] = None) -> None:
        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)

        operator_states: dict[Operator, DeploymentState] = \
            {operator: DeploymentState.Unknown for operator in pipeline.get_protected().get_nested_instances().values()}

        finished: bool = False

        while not finished:
            for operator in pipeline.get_protected().get_nested_instances().values():
                operator_state = self.retrieve_operator_state(operator.get_full_name())

                if operator_state != operator_states[operator]:
                    operator_states[operator] = operator_state
                    if on_operator_state_change is not None:
                        on_operator_state_change(operator, operator_state)

            if any((DeploymentState.Open == state) or
                   (DeploymentState.Running == state) for state in operator_states.values()):
                time.sleep(2)
            else:
                finished = True
