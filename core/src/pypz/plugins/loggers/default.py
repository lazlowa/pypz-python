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
from typing import Any

from pypz.core.commons.loggers import DefaultContextLogger
from pypz.core.commons.parameters import OptionalParameter
from pypz.core.specs.plugin import LoggerPlugin


class DefaultLoggerPlugin(LoggerPlugin, DefaultContextLogger):
    """
    This is the default implementation of the :class:`LoggerPlugin <pypz.core.specs.plugin.LoggerPlugin>`
    interface. It actually does not implement anything, but uses the
    :class:`DefaultContextLogger <pypz.core.commons.loggers.DefaultContextLogger>`. This is
    possible, since both implements the
    :class:`ContextLoggerInterface <pypz.core.commons.loggers.ContextLoggerInterface>`.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    _log_level = OptionalParameter(str,
                                   alt_name="logLevel",
                                   on_update=lambda instance, val: None
                                   if val is None else instance.set_log_level(val))

    def __init__(self, name: str = None, *args, **kwargs):
        LoggerPlugin.__init__(self, name, *args, **kwargs)
        DefaultContextLogger.__init__(self, self.get_full_name())

        self._log_level = "INFO"

    def _on_interrupt(self, system_signal: int = None) -> None:
        pass

    def _on_error(self, source: Any, exception: Exception) -> None:
        pass
