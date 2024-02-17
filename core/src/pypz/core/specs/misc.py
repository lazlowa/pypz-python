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
from typing import Optional, Any, TypeVar

from pypz.core.specs.instance import Instance, NestedInstanceType
from pypz.core.specs.operator import Operator
from pypz.core.specs.plugin import Plugin, PortPlugin, InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, \
    ServicePlugin


BlankNestedInstanceType = TypeVar('BlankNestedInstanceType', bound='BlankInstance')


class BlankInstance(Instance[BlankNestedInstanceType]):

    def _on_interrupt(self, system_signal: int = None) -> None:
        pass

    def _on_error(self) -> None:
        pass


class BlankPlugin(BlankInstance[None]):
    pass


class BlankResourceHandlerPlugin(ResourceHandlerPlugin, BlankPlugin):

    def _on_resource_creation(self) -> bool:
        return True

    def _on_resource_deletion(self) -> bool:
        return True


class BlankPortPlugin(PortPlugin, BlankPlugin):

    def _on_port_open(self) -> bool:
        return True

    def _on_port_close(self) -> bool:
        return True


class BlankInputPortPlugin(InputPortPlugin, BlankPortPlugin):

    def can_retrieve(self) -> bool:
        return False

    def retrieve(self) -> Any:
        pass

    def commit_current_read_offset(self) -> None:
        pass


class BlankOutputPortPlugin(OutputPortPlugin, BlankPortPlugin):

    def send(self, data: Any) -> Any:
        pass


class BlankServicePlugin(ServicePlugin, BlankPlugin):

    def _on_service_start(self) -> bool:
        return True

    def _on_service_shutdown(self) -> bool:
        return True


class BlankOperator(Operator, BlankInstance[Plugin]):

    def __init__(self, name: str = None, replication_factor: int = None, *args, **kwargs):
        super().__init__(name, replication_factor, *args, **kwargs)

    def _on_init(self) -> bool:
        return True

    def _on_running(self) -> Optional[bool]:
        return True

    def _on_shutdown(self) -> bool:
        return True
