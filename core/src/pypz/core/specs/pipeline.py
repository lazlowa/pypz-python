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
from typing import cast

import yaml

from pypz.core.specs.dtos import PipelineInstanceDTO, PipelineSpecDTO
from pypz.core.specs.instance import Instance, RegisteredInterface
from pypz.core.specs.operator import Operator


class Pipeline(Instance[Operator], RegisteredInterface):
    """
    This class represents the pipeline instance specs. A pipeline is actually a
    virtual organization of operators. It has a meaning only on pipeline level
    actions like deployment and execution. A pipeline spec can contain
    operators as nested instance.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, Operator, *args, **kwargs)

    def get_dto(self) -> PipelineInstanceDTO:
        instance_dto = super().get_dto()

        # Replicas must be excluded from the dto
        instance_dto.spec.nestedInstances = {
            operator.get_dto() for operator in self.get_protected().get_nested_instances().values()
            if operator.is_principal()
        }

        return PipelineInstanceDTO(name=instance_dto.name,
                                   parameters=instance_dto.parameters,
                                   dependsOn=instance_dto.dependsOn,
                                   spec=PipelineSpecDTO(**instance_dto.spec.__dict__))

    @staticmethod
    def create_from_string(source, *args, **kwargs) -> 'Pipeline':
        return Pipeline.create_from_dto(PipelineInstanceDTO(**yaml.safe_load(source)), *args, **kwargs)

    @staticmethod
    def create_from_dto(instance_dto: 'PipelineInstanceDTO', *args, **kwargs) -> 'Pipeline':
        return cast(Pipeline, Instance.create_from_dto(instance_dto, *args, **kwargs))

    def _on_interrupt(self, system_signal: int = None) -> None:
        pass

    def _on_error(self) -> None:
        pass

