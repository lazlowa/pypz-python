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


class DeploymentConflictException(Exception):
    pass


class DeploymentNotFoundException(Exception):
    pass


class DeploymentException(Exception):
    pass


label_key_instance_type = "pypz.io/instance-type"
label_key_exec_mode = "pypz.io/exec-mode"
label_key_part_of = "pypz.io/part-of"
label_key_instance_name = "pypz.io/instance-name"
label_value_pipeline = "pipeline"
label_value_operator = "operator"
pipeline_config_secret_key = "pipeline-config"
env_var_operator_name = "PYPZ_OPERATOR_INSTANCE_NAME"
env_var_operator_exec_mode = "PYPZ_OPERATOR_EXEC_MODE"


def sanitize(string: str) -> str:
    return string.translate(str.maketrans({"_": "-", ".": "-"}))  # type: ignore
