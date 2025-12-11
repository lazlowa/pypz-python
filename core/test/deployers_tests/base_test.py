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
import unittest

from pypz.core.specs.operator import Operator
from pypz.deployers.base import DeploymentState

from core.test.deployers_tests.resources import TestDeployer, TestPipeline


class DeployerTest(unittest.TestCase):

    def test_deployer_is_all_operator_in_state_all(self):
        pipeline = TestPipeline("pipeline")
        deployer = TestDeployer()

        deployer.deploy(pipeline)

        self.assertTrue(
            deployer.is_all_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Running
            )
        )
        self.assertTrue(
            deployer.is_all_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Running, DeploymentState.Open
            )
        )
        self.assertFalse(
            deployer.is_all_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Open
            )
        )

        deployer.deployed_operators[pipeline.operator_a.get_full_name()] = (
            DeploymentState.Unknown
        )

        self.assertFalse(
            deployer.is_all_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Running
            )
        )
        self.assertFalse(
            deployer.is_all_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Running, DeploymentState.Open
            )
        )
        self.assertFalse(
            deployer.is_all_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Open
            )
        )

    def test_deployer_is_any_operator_in_state(self):
        pipeline = TestPipeline("pipeline")
        deployer = TestDeployer()

        deployer.deploy(pipeline)

        self.assertTrue(
            deployer.is_any_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Running
            )
        )
        self.assertTrue(
            deployer.is_any_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Running, DeploymentState.Open
            )
        )
        self.assertFalse(
            deployer.is_any_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Open
            )
        )

        deployer.deployed_operators[pipeline.operator_a.get_full_name()] = (
            DeploymentState.Open
        )

        self.assertTrue(
            deployer.is_any_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Open
            )
        )

    def test_deployer_attach_with_all_running(self):
        pipeline = TestPipeline("pipeline")
        deployer = TestDeployer()

        def on_operator_state_change(operator: Operator, state: DeploymentState):
            if DeploymentState.Running == state:
                deployer.deployed_operators[operator.get_full_name()] = (
                    DeploymentState.Completed
                )

        deployer.deploy(pipeline)

        deployer.attach(pipeline.get_full_name(), on_operator_state_change)

        self.assertTrue(
            deployer.is_all_operator_in_state(
                pipeline.get_full_name(), DeploymentState.Completed
            )
        )
