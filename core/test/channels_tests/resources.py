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
import time
from typing import TYPE_CHECKING, Any

from pypz.core.channels.base import ChannelBase
from pypz.core.channels.io import ChannelReader, ChannelWriter
from pypz.core.specs.misc import (
    BlankInputPortPlugin,
    BlankOperator,
    BlankOutputPortPlugin,
)

if TYPE_CHECKING:
    from pypz.core.specs.plugin import InputPortPlugin, OutputPortPlugin, PortPlugin


class TestGroupedOperator(BlankOperator):

    def __init__(
        self, name: str = None, replication_factor: int = None, *args, **kwargs
    ):
        super().__init__(name, replication_factor, *args, **kwargs)

        self.input_port = BlankInputPortPlugin()
        self.input_port_1 = BlankInputPortPlugin()
        self.output_port = BlankOutputPortPlugin()


class TestChannel(ChannelBase):

    def __init__(
        self,
        channel_name: str,
        context: "PortPlugin",
        executor: concurrent.futures.ThreadPoolExecutor = None,
        **kwargs,
    ):
        super().__init__(channel_name, context, executor, **kwargs)

    def _create_resources(self) -> bool:
        pass

    def _delete_resources(self) -> bool:
        pass

    def _open_channel(self) -> bool:
        pass

    def _close_channel(self) -> bool:
        pass

    def _configure_channel(self, channel_configuration: dict):
        pass

    def _send_status_message(self, message: str):
        pass

    def _retrieve_status_messages(self) -> list:
        pass


TEST_DATA_TRANSFER_MEDIUM: dict[str, list | None] = {}
TEST_CONTROL_TRANSFER_MEDIUM: dict[str, list | None] = {}
TEST_DATA_OFFSET_STORE: dict[str, int] = {}
OUTPUT_STATE_POSTFIX = ".output.state"
INPUT_STATE_POSTFIX = ".input.state"

PARAM_PREFIX_RETURN_MODIFIER = "return_"
PARAM_PREFIX_SLEEP_MODIFIER = "sleep_"
PARAM_PREFIX_RAISE_MODIFIER = "raise_"


class TestChannelWriter(ChannelWriter):

    def __init__(
        self,
        channel_name: str,
        context: "OutputPortPlugin",
        executor: concurrent.futures.ThreadPoolExecutor = None,
        **kwargs,
    ):
        super().__init__(channel_name, context, executor, **kwargs)

        self.status_message_offset: int = 0

    def _write_records(self, records: list[Any]) -> None:
        TEST_DATA_TRANSFER_MEDIUM[self._channel_name].extend(records)

    def _create_resources(self) -> bool:
        ret_val = self.control_handler(
            TestChannelWriter._create_resources.__name__, True
        )
        if ret_val:
            TEST_DATA_TRANSFER_MEDIUM[self._channel_name] = None
            TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + OUTPUT_STATE_POSTFIX] = (
                None
            )
            TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + INPUT_STATE_POSTFIX] = (
                None
            )
        return ret_val

    def _delete_resources(self) -> bool:
        ret_val = self.control_handler(
            TestChannelWriter._delete_resources.__name__, True
        )
        if ret_val:
            del TEST_DATA_TRANSFER_MEDIUM[self._channel_name]
            del TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + OUTPUT_STATE_POSTFIX]
            del TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + INPUT_STATE_POSTFIX]
        return ret_val

    def _open_channel(self) -> bool:
        ret_val = self.control_handler(TestChannelWriter._open_channel.__name__, True)
        if ret_val:
            TEST_DATA_TRANSFER_MEDIUM[self._channel_name] = []
            TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + OUTPUT_STATE_POSTFIX] = []
            TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + INPUT_STATE_POSTFIX] = []
        return ret_val

    def _close_channel(self) -> bool:
        ret_val = self.control_handler(TestChannelWriter._close_channel.__name__, True)
        if ret_val:
            TEST_DATA_TRANSFER_MEDIUM[self._channel_name] = None
            TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + OUTPUT_STATE_POSTFIX] = (
                None
            )
            TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + INPUT_STATE_POSTFIX] = (
                None
            )
        return ret_val

    def _configure_channel(self, channel_configuration: dict):
        pass

    def _send_status_message(self, message: str):
        TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + OUTPUT_STATE_POSTFIX].append(
            message
        )

    def _retrieve_status_messages(self) -> list:
        retrieved = TEST_CONTROL_TRANSFER_MEDIUM[
            self._channel_name + INPUT_STATE_POSTFIX
        ][self.status_message_offset :]
        self.status_message_offset += len(retrieved)
        return retrieved

    def can_delete_resources(self) -> bool:
        return True

    def can_close(self) -> bool:
        return True

    def control_handler(self, method_name: str, default_return_value=None):
        if PARAM_PREFIX_RAISE_MODIFIER + method_name in self.get_configuration():
            raise AttributeError(
                self.get_configuration()[PARAM_PREFIX_RAISE_MODIFIER + method_name]
            )

        if PARAM_PREFIX_SLEEP_MODIFIER + method_name in self.get_configuration():
            time.sleep(
                self.get_configuration()[PARAM_PREFIX_SLEEP_MODIFIER + method_name]
            )

        if PARAM_PREFIX_RETURN_MODIFIER + method_name in self.get_configuration():
            return self.get_configuration()[PARAM_PREFIX_RETURN_MODIFIER + method_name]

        return default_return_value


class TestChannelReader(ChannelReader):

    def __init__(
        self,
        channel_name: str,
        context: "InputPortPlugin",
        executor: concurrent.futures.ThreadPoolExecutor = None,
        **kwargs,
    ):
        super().__init__(channel_name, context, executor, **kwargs)

        self.output_status_message_offset: int = 0
        self.input_status_message_offset: int = 0

    def has_records(self) -> bool:
        return 0 < len(
            TEST_DATA_TRANSFER_MEDIUM[self._channel_name][
                self.get_read_record_offset() :
            ]
        )

    def _load_input_record_offset(self) -> int:
        if self._channel_name not in TEST_DATA_OFFSET_STORE:
            return 0

        return TEST_DATA_OFFSET_STORE[self._channel_name]

    def _read_records(self) -> list[Any]:
        retrieved = TEST_DATA_TRANSFER_MEDIUM[self._channel_name][
            self.get_read_record_offset() :
        ]
        return retrieved

    def _commit_offset(self, offset: int) -> None:
        TEST_DATA_OFFSET_STORE[self._channel_name] = offset

    def _create_resources(self) -> bool:
        return self.control_handler(TestChannelReader._create_resources.__name__, True)

    def _delete_resources(self) -> bool:
        return self.control_handler(TestChannelReader._delete_resources.__name__, True)

    def _open_channel(self) -> bool:
        if (
            (self._channel_name in TEST_DATA_TRANSFER_MEDIUM)
            and (TEST_DATA_TRANSFER_MEDIUM[self._channel_name] is not None)
            and self.control_handler(TestChannelReader._open_channel.__name__, True)
        ):
            return True
        return False

    def _close_channel(self) -> bool:
        return self.control_handler(TestChannelReader._close_channel.__name__, True)

    def _configure_channel(self, channel_configuration: dict) -> None:
        pass

    def _send_status_message(self, message: str):
        TEST_CONTROL_TRANSFER_MEDIUM[self._channel_name + INPUT_STATE_POSTFIX].append(
            message
        )

    def _retrieve_status_messages(self) -> list:
        retrieved = TEST_CONTROL_TRANSFER_MEDIUM[
            self._channel_name + OUTPUT_STATE_POSTFIX
        ][self.output_status_message_offset :]
        self.output_status_message_offset += len(retrieved)

        if 1 < self._context.get_group_size():
            retrieved_input_status = TEST_CONTROL_TRANSFER_MEDIUM[
                self._channel_name + INPUT_STATE_POSTFIX
            ][
                self.input_status_message_offset :
            ]  # noqa: E501
            self.input_status_message_offset += len(retrieved_input_status)
            retrieved.extend(retrieved_input_status)

        return retrieved

    def can_delete_resources(self) -> bool:
        return True

    def can_close(self) -> bool:
        return True

    def control_handler(self, method_name: str, default_return_value=None):
        if PARAM_PREFIX_RAISE_MODIFIER + method_name in self.get_configuration():
            raise AttributeError(
                self.get_configuration()[PARAM_PREFIX_RAISE_MODIFIER + method_name]
            )

        if PARAM_PREFIX_SLEEP_MODIFIER + method_name in self.get_configuration():
            time.sleep(
                self.get_configuration()[PARAM_PREFIX_SLEEP_MODIFIER + method_name]
            )

        if PARAM_PREFIX_RETURN_MODIFIER + method_name in self.get_configuration():
            return self.get_configuration()[PARAM_PREFIX_RETURN_MODIFIER + method_name]

        return default_return_value
