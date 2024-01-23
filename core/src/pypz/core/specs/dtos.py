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
from typing import Optional, Type, TypeVar

NestedInstanceDTOType = TypeVar("NestedInstanceDTOType", bound='InstanceDTO')


class SpecDTO:
    def __init__(self,
                 name: str = None,
                 location: str = None,
                 expectedParameters: dict = None,
                 types: list = None,
                 nestedInstanceType: str = None,
                 nestedInstances: list[NestedInstanceDTOType] = None,
                 nested_instance_dto_type: Optional[Type[NestedInstanceDTOType]] = 'default'):
        self.name: str = name
        self.location: str = location
        self.expectedParameters: dict = expectedParameters
        self.types: list = types
        self.nestedInstanceType: str = nestedInstanceType
        self.nestedInstances: Optional[set[NestedInstanceDTOType]] = None

        if nested_instance_dto_type == 'default':
            nested_instance_dto_type = InstanceDTO

        if (nestedInstances is not None) and (nested_instance_dto_type is not None):
            self.nestedInstances: list[NestedInstanceDTOType] = list()
            if isinstance(nestedInstances, (set, list)):
                for nestedInstance in nestedInstances:
                    if isinstance(nestedInstance, InstanceDTO):
                        self.nestedInstances.append(nestedInstance)
                    elif isinstance(nestedInstance, dict):
                        self.nestedInstances.append(nested_instance_dto_type(**nestedInstance))
                    else:
                        raise TypeError(f"Invalid nestedInstance type: {type(nestedInstance)}")
            else:
                raise TypeError(f"Invalid nestedInstances type: {type(nestedInstances)}")

    def __eq__(self, other):
        if self is other:
            return True

        return isinstance(other, type(self)) and \
            (self.name == other.name) and \
            (self.location == other.location) and \
            (self.expectedParameters == other.expectedParameters) and \
            (self.types == other.types) and \
            (self.nestedInstanceType == other.nestedInstanceType) and \
            (self.nestedInstances == other.nestedInstances)

    def __hash__(self):
        return hash(self.name)


class InstanceDTO:
    def __init__(self, name: str = None, parameters: dict = None, dependsOn: list[str] = None,
                 spec: SpecDTO = None):
        self.name: str = name
        self.parameters: dict = parameters
        self.dependsOn: list[str] = dependsOn

        if (spec is None) or isinstance(spec, SpecDTO):
            self.spec = spec
        elif isinstance(spec, dict):
            self.spec = SpecDTO(**spec)
        else:
            raise TypeError(f"Invalid instance spec type: {type(spec)}")

    def __eq__(self, other):
        if self is other:
            return True

        return isinstance(other, type(self)) and \
            (self.name == other.name) and \
            (self.parameters == other.parameters) and \
            (self.dependsOn == other.dependsOn) and \
            (self.spec == other.spec)

    def __hash__(self):
        return hash((self.name, self.spec.__hash__()))


class PluginSpecDTO(SpecDTO):
    def __init__(self,
                 name: str = None,
                 location: str = None,
                 expectedParameters: dict = None,
                 types: list = None,
                 nestedInstanceType: str = None,
                 nestedInstances: list = None):
        super().__init__(name, location, expectedParameters, types, nestedInstanceType, None, None)


class PluginInstanceDTO(InstanceDTO):
    def __init__(self,
                 name: str = None,
                 parameters: dict = None,
                 dependsOn: list[str] = None,
                 spec: PluginSpecDTO = None):
        super().__init__(name, parameters, dependsOn, None)

        if (spec is None) or isinstance(spec, PluginSpecDTO):
            self.spec = spec
        elif isinstance(spec, dict):
            self.spec = PluginSpecDTO(**spec)
        else:
            raise TypeError(f"Invalid instance spec type: {type(spec)}")


class OperatorSpecDTO(SpecDTO):
    def __init__(self,
                 name: str = None,
                 location: str = None,
                 expectedParameters: dict = None,
                 types: list = None,
                 nestedInstanceType: str = None,
                 nestedInstances: list[PluginInstanceDTO] = None):
        super().__init__(name, location, expectedParameters, types, nestedInstanceType, nestedInstances, PluginInstanceDTO)


class OperatorConnectionSource:
    def __init__(self,
                 instanceName: str = None,
                 outputPortName: str = None):
        self.instanceName: str = instanceName
        self.outputPortName: str = outputPortName

    def __eq__(self, other):
        if self is other:
            return True

        return isinstance(other, type(self)) and \
            (self.instanceName == other.instanceName) and \
            (self.outputPortName == other.outputPortName)

    def __hash__(self):
        return hash((self.instanceName, self.outputPortName))


class OperatorConnection:
    def __init__(self,
                 inputPortName: str = None,
                 source: dict | OperatorConnectionSource = None):
        self.inputPortName: str = inputPortName
        self.source: OperatorConnectionSource = source \
            if isinstance(source, OperatorConnectionSource) \
            else OperatorConnectionSource(**source)

    def __eq__(self, other):
        if self is other:
            return True

        return isinstance(other, type(self)) and \
            (self.inputPortName == other.inputPortName) and \
            (self.source == other.source)

    def __hash__(self):
        return hash((self.inputPortName, self.source.__hash__()))


class OperatorInstanceDTO(InstanceDTO):
    """
    Operator instance related DTO. Operator instances have additional instance
    information that shall be modelled via the DTO e.g., since Operator
    instances can be connected through port plugins, those connections shall
    be modelled as well.
    """

    def __init__(self,
                 name: str = None,
                 parameters: dict = None,
                 dependsOn: list[str] = None,
                 spec: OperatorSpecDTO = None,
                 connections: list[OperatorConnection] = None):
        super().__init__(name, parameters, dependsOn, None)

        if connections is not None:
            self.connections: Optional[list[OperatorConnection]] = list()
            if isinstance(connections, (set, list)):
                for connection in connections:
                    if isinstance(connection, OperatorConnection):
                        self.connections.append(connection)
                    elif isinstance(connection, dict):
                        self.connections.append(OperatorConnection(**connection))
                    else:
                        raise TypeError(f"Invalid nestedInstance type: {type(connection)}")
            else:
                raise TypeError(f"Invalid nestedInstances type: {type(connections)}")

        else:
            self.connections: Optional[set[OperatorConnection]] = None

        if (spec is None) or isinstance(spec, OperatorSpecDTO):
            self.spec = spec
        elif isinstance(spec, dict):
            self.spec = OperatorSpecDTO(**spec)
        else:
            raise TypeError(f"Invalid instance spec type: {type(spec)}")

    def __eq__(self, other):
        if self is other:
            return True

        return super().__eq__(other) and \
            (self.connections == other.connections)

    def __hash__(self):
        return super().__hash__()


class PipelineSpecDTO(SpecDTO):
    def __init__(self,
                 name: str = None,
                 location: str = None,
                 expectedParameters: dict = None,
                 types: list = None,
                 nestedInstanceType: str = None, nestedInstances: list[OperatorInstanceDTO] = None):
        super().__init__(name, location, expectedParameters, types, nestedInstanceType, nestedInstances, OperatorInstanceDTO)


class PipelineInstanceDTO(InstanceDTO):
    def __init__(self,
                 name: str = None,
                 parameters: dict = None,
                 dependsOn: list[str] = None,
                 spec: PipelineSpecDTO = None):
        super().__init__(name, parameters, dependsOn, None)

        if (spec is None) or isinstance(spec, PipelineSpecDTO):
            self.spec = spec
        elif isinstance(spec, dict):
            self.spec = PipelineSpecDTO(**spec)
        else:
            raise TypeError(f"Invalid instance spec type: {type(spec)}")
