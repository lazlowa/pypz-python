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
from typing import Any, Optional

from pypz.abstracts.channel_ports import ChannelInputPort, ChannelOutputPort
from pypz.core.channels.misc import BlankChannelReader, BlankChannelWriter


class BlankChannelInputPort(ChannelInputPort):

    def __init__(self, name: str = None, schema: Any = None, group_mode: bool = False, *args, **kwargs):
        super().__init__(name, schema, group_mode, BlankChannelReader, *args, **kwargs)


class BlankChannelOutputPort(ChannelOutputPort):
    def __init__(self, name: str = None, schema: Optional[Any] = None, *args, **kwargs):
        super().__init__(name, schema, BlankChannelWriter, *args, **kwargs)
