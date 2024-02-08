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
import logging
import sys
from abc import ABC, abstractmethod
from typing import Any, Optional

import colorlog
from colorlog import ColoredFormatter


class ContextLoggerInterface(ABC):
    """
    This interface provides the necessary methods to be able to implement
    the context logging functionality. Context logging means that there
    is exactly one logger, which will be (re)used in the given context.
    """

    @abstractmethod
    def _error(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        pass

    @abstractmethod
    def _warn(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        pass

    @abstractmethod
    def _info(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        pass

    @abstractmethod
    def _debug(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        pass

    def set_log_level(self, log_level: Any) -> None:
        pass


class ContextLogger(ContextLoggerInterface):
    """
    This class has the responsibility to provide a simple logging interface, which
    hides the complexity of maintaining the context information. The user can
    call the familiar methods without knowing, how the context information are
    handled by the protected methods.

    By expecting a logger in the constructor we are allowing to reuse existing loggers
    from higher contexts i.e., a top-level instance can provide it's logger to the
    nested instances along with the context information. The nested instance can
    further forward to it's nested instances and so on. Each instance provides itself
    to the context stack so, if a logger method of the actual context will be called,
    then the logger itself will get the context information automatically.

    :param logger: a logger implementation that will be used to route the messages to
    :param context_stack: the information about the current and previous contexts
    """

    def __init__(self, logger: ContextLoggerInterface, *context_stack: str):
        self._logger: ContextLoggerInterface = logger
        """
        The provided logger to use on this context
        """

        self._context_stack: list[str] = [*context_stack]
        """
        The context stack, which contains the actual and all parent contexts
        """

    def get_context_stack(self) -> list[str]:
        return self._context_stack

    def _error(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger._error(event, context_stack, *args, **kw)

    def _warn(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger._warn(event, context_stack, *args, **kw)

    def _info(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger._info(event, context_stack, *args, **kw)

    def _debug(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger._debug(event, context_stack, *args, **kw)

    def set_log_level(self, log_level: str | int) -> None:
        self._logger.set_log_level(log_level)

    def error(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self._error(event, self._context_stack, *args, **kw)

    def warn(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self._warn(event, self._context_stack, *args, **kw)

    def info(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self._info(event, self._context_stack, *args, **kw)

    def debug(self, event: Optional[str] = None, *args: Any, **kw: Any) -> Any:
        self._debug(event, self._context_stack, *args, **kw)


class DefaultContextLogger(ContextLoggerInterface):
    """
    The default implementation of the :class:`ContextLoggerInterface <pypz.core.commons.loggers.ContextLoggerInterface>`,
    which will send log messages to the standard out.

    :param name: name of the instance, if not provided, it will be attempted to deduce from the variable's name
    """

    def __init__(self, name: str = None):
        logging.basicConfig()
        self._logger: logging.Logger = colorlog.getLogger(name)
        self._logger.propagate = False

        if not self._logger.handlers:
            handler = colorlog.StreamHandler(sys.stdout)
            formatter = ColoredFormatter(
                "%(log_color)s%(levelname)s | %(asctime)s | %(name)s | %(context)s | %(message)s",
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'light_black',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def _error(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger.error(event,
                           extra={
                               "context": " | ".join(context_stack) if context_stack is not None else None
                           },
                           *args, **kw)

    def _warn(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger.warning(event,
                             extra={
                                 "context": " | ".join(context_stack) if context_stack is not None else None
                             },
                             *args, **kw)

    def _info(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger.info(event,
                          extra={
                              "context": " | ".join(context_stack) if context_stack is not None else None
                          },
                          *args, **kw)

    def _debug(self, event: Optional[str] = None, context_stack: list[str] = None, *args: Any, **kw: Any) -> Any:
        self._logger.debug(event,
                           extra={
                               "context": " | ".join(context_stack) if context_stack is not None else None
                           },
                           *args, **kw)

    def set_log_level(self, log_level: str | int) -> None:
        if isinstance(log_level, str):
            self._logger.setLevel(logging.getLevelName(log_level.upper()))
        elif isinstance(log_level, int):
            self._logger.setLevel(log_level)
        else:
            raise TypeError(f"Invalid log_level type: {log_level}")
