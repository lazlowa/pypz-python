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
    Open = ("Open",)
    Running = ("Running",)
    Completed = ("Completed",)
    Failed = ("Failed",)
    Unhealthy = ("Unhealthy",)
    Unknown = ("Unknown",)
    NotExisting = "NotExisting"


class Deployer(ABC):
    """
    This is the base class for every deployer implementations. If you want to
    develop your own deployer with your own choice of technology, then you need
    to implement this interface.
    """

    # ================== abstract methods ====================

    @abstractmethod
    def deploy(
        self,
        pipeline: Pipeline,
        execution_mode: ExecutionMode = ExecutionMode.Standard,
        ignore_operators: list[Operator] = None,
        wait: bool = True,
    ) -> None:
        """
        Shall implement the logic to deploy a pipeline by its instance.

        :param pipeline: pipeline instance to be deployed
        :param execution_mode: execution mode of the operators (check ExecutionMode for details)
        :param ignore_operators: list of operator instance to be excluded from the deployment
        :param wait: True - block until completion; False - don't block until completion
        """

        pass

    @abstractmethod
    def destroy(
        self, pipeline_name: str, force: bool = False, wait: bool = True
    ) -> None:
        """
        Shall implement the logic to destroy a pipeline by its name.

        :param pipeline_name: name of the deployed pipeline entity
        :param force: True - kill without grace period; False - with grace period
        :param wait: True - block until completion; False - don't block until completion
        """

        pass

    @abstractmethod
    def restart_operator(
        self, operator_full_name: str, force: bool = False, wait: bool = True
    ) -> None:
        """
        Shall implement the logic to restart a single operator in a pipeline by its name.
        If the operator does not exist, it shall rather create it without throwing an exception.

        :param operator_full_name: full name of the deployed operator
        :param force: True - kill without grace period; False - with grace period
        :param wait: True - block until completion; False - don't block until completion
        """

        pass

    @abstractmethod
    def destroy_operator(
        self, operator_full_name: str, force: bool = False, wait: bool = True
    ) -> None:
        """
        Shall implement the logic to destroy a single operator by its name.

        :param operator_full_name: full name of the deployed operator
        :param force: True - kill without grace period; False - with grace period
        :param wait: True - block until completion; False - don't block until completion
        """

        pass

    @abstractmethod
    def is_deployed(self, pipeline_name: str) -> bool:
        """
        Shall implement the logic to check, if a pipeline with the specified name has been deployed.

        :param pipeline_name: name of the pipeline to check
        :return: True, if deployed, False if not
        """

        pass

    @abstractmethod
    def retrieve_pipeline_deployments(self) -> set[str]:
        """
        Shall implement the logic to get the names of all deployed pipelines.

        :return: set of names of the deployed pipelines
        """
        pass

    @abstractmethod
    def retrieve_deployed_pipeline(self, pipeline_name: str) -> Optional[Pipeline]:
        """
        Shall implement the logic to retrieve and create the deployed pipeline instance by its name.

        :param pipeline_name: name of the pipeline
        :return: Pipeline object, if existing, None if not existing
        """
        pass

    @abstractmethod
    def retrieve_operator_state(self, operator_full_name: str) -> DeploymentState:
        """
        Shall implement the logic to retrieve the state of a single operator by its name.

        :param operator_full_name: full name of the deployed operator
        :return: check DeploymentState for details
        """
        pass

    @abstractmethod
    def retrieve_operator_logs(self, operator_full_name: str) -> Optional[str]:
        """
        Shall implement the logic to retrieve the logs from a deployed operator by its name.

        :param operator_full_name: full name of the deployed operator
        :return: operator logs as string
        """
        pass

    # ================== public methods ====================

    def retrieve_pipeline_state(self, pipeline_name: str) -> dict[str, DeploymentState]:
        """
        This method retrieves and collects all the operators' states in the deployed pipeline.

        :param pipeline_name: name of the deployed pipeline entity
        :return: a dict, where key is the name of the operator and the value is the corresponding state object
        """

        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)

        operator_states = {}
        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_states[operator.get_full_name()] = self.retrieve_operator_state(
                operator.get_full_name()
            )

        return operator_states

    def is_any_operator_in_state(self, pipeline_name: str, *state: DeploymentState):
        """
        This method checks, if **any** of the operators in the deployed pipeline is in **any** of the
        specified states.

        :param pipeline_name: name of the deployed pipeline entity
        :param state: list of states in OR condition
        """

        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)
        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_state = self.retrieve_operator_state(operator.get_full_name())
            if operator_state in state:
                return True
        return False

    def is_all_operator_in_state(self, pipeline_name: str, *state: DeploymentState):
        """
        This method checks, if **all** the operators in the deployed pipeline is in **any** of the
        specified states.

        :param pipeline_name: name of the deployed pipeline entity
        :param state: list of states in OR condition
        """

        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)
        for operator in pipeline.get_protected().get_nested_instances().values():
            operator_state = self.retrieve_operator_state(operator.get_full_name())
            if operator_state not in state:
                return False
        return True

    def attach(
        self,
        pipeline_name: str,
        on_operator_state_change: Callable[[Operator, DeploymentState], None] = None,
    ) -> None:
        """
        This method attaches itself to a deployed pipeline and remains attached until the pipeline is
        not finished. It is possible to specify callback functions to hook into certain state
        changes. If that state change happens, then the callback gets the Operator instance and
        the corresponding state provided.

        :param pipeline_name: name of the deployed pipeline entity
        :param on_operator_state_change: callback to hook into state changes
        """
        pipeline: Pipeline = self.retrieve_deployed_pipeline(pipeline_name)

        operator_states: dict[Operator, DeploymentState] = {
            operator: DeploymentState.Unknown
            for operator in pipeline.get_protected().get_nested_instances().values()
        }

        finished: bool = False

        while not finished:
            for operator in pipeline.get_protected().get_nested_instances().values():
                operator_state = self.retrieve_operator_state(operator.get_full_name())

                if operator_state != operator_states[operator]:
                    operator_states[operator] = operator_state
                    if on_operator_state_change is not None:
                        on_operator_state_change(operator, operator_state)

            if any(
                (DeploymentState.Open == state) or (DeploymentState.Running == state)
                for state in operator_states.values()
            ):
                time.sleep(2)
            else:
                finished = True
