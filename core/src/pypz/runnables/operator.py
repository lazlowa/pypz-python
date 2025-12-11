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
import sys

import yaml
from pypz.core.specs.dtos import PipelineInstanceDTO
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.executors.commons import ExecutionMode
from pypz.executors.operator.executor import OperatorExecutor

""" This script is used to invoke operator execution """
if __name__ == "__main__":
    print(sys.argv)
    if 3 > len(sys.argv):
        print(
            "Missing arguments:\n"
            "$1 - path to config file\n"
            "$2 - operator simple name\n"
            f"$3 - [Optional] ExecutionMode (default: {ExecutionMode.Standard.name}). "
            f"Possible values: {[elem.name for elem in ExecutionMode]}",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(sys.argv[1]) as json_file:
        pipeline_dto: PipelineInstanceDTO = PipelineInstanceDTO(
            **yaml.safe_load(json_file)
        )

        pipeline: Pipeline = Pipeline.create_from_dto(
            pipeline_dto, mock_nonexistent=True
        )
        operator: Operator = pipeline.get_protected().get_nested_instance(sys.argv[2])
        exec_mode: ExecutionMode = (
            ExecutionMode.Standard if 4 > len(sys.argv) else ExecutionMode(sys.argv[3])
        )

        print(
            f"Operator to execute: {operator.get_full_name()}; Execution mode: {exec_mode.name}"
        )

        executor: OperatorExecutor = OperatorExecutor(operator)

        exit_code = executor.execute(exec_mode)

        sys.exit(exit_code)
