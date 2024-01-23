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
    def __init__(self, logger: ContextLoggerInterface, *context_stack: str):
        self._logger: ContextLoggerInterface = logger
        self._context_stack: list[str] = [*context_stack]

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

    def __init__(self, name: str):
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
