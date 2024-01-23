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
import concurrent.futures
from typing import Any, TYPE_CHECKING, Optional

from pypz.core.channels.io import ChannelReader, ChannelWriter

if TYPE_CHECKING:
    from pypz.core.specs.plugin import InputPortPlugin, OutputPortPlugin


class BlankChannelReader(ChannelReader):
    def __init__(self, channel_name: str,
                 context: 'InputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

    def _load_input_record_offset(self) -> int:
        return 0

    def has_records(self) -> bool:
        return False

    def _read_records(self) -> list[Any]:
        return []

    def _commit_offset(self, offset: int) -> None:
        pass

    def _create_resources(self) -> bool:
        return True

    def _delete_resources(self) -> bool:
        return True

    def _open_channel(self) -> bool:
        return True

    def _close_channel(self) -> bool:
        return True

    def _configure_channel(self, channel_configuration: dict) -> None:
        pass

    def _send_status_message(self, message: str) -> None:
        pass

    def _retrieve_status_messages(self) -> list:
        return []


class BlankChannelWriter(ChannelWriter):

    def __init__(self, channel_name: str,
                 context: 'OutputPortPlugin',
                 executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
                 **kwargs):
        super().__init__(channel_name, context, executor, **kwargs)

    def _write_records(self, records: list[Any]) -> None:
        pass

    def _create_resources(self) -> bool:
        return True

    def _delete_resources(self) -> bool:
        return True

    def _open_channel(self) -> bool:
        return True

    def _close_channel(self) -> bool:
        return True

    def _configure_channel(self, channel_configuration: dict) -> None:
        pass

    def _send_status_message(self, message: str) -> None:
        pass

    def _retrieve_status_messages(self) -> list:
        return []
