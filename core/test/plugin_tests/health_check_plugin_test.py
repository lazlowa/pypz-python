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
import time
import unittest
from http.client import HTTPConnection
from threading import Thread
from typing import Any, Optional

from pypz.core.specs.operator import Operator
from pypz.executors.operator.executor import OperatorExecutor
from pypz.plugins.misc.health_check import HttpHealthCheckPlugin


class TestOperator(Operator):
    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)

        self.health_check = HttpHealthCheckPlugin()

    def _on_init(self) -> bool:
        return True

    def _on_running(self) -> Optional[bool]:
        time.sleep(5)
        return True

    def _on_shutdown(self) -> bool:
        return True

    def _on_interrupt(self, system_signal: int = None) -> None:
        pass

    def _on_error(self, source: Any, exception: Exception) -> None:
        pass


class HealthCheckPluginTest(unittest.TestCase):

    def test_endpoint_active_expect_success(self):
        operator = TestOperator(name="op")
        executor = OperatorExecutor(operator)
        executor.execute()
        executor_thread = Thread(
            target=executor.execute,
            name="operator-executor",
            daemon=True,
        )
        executor_thread.start()
        conn = HTTPConnection(
            "localhost", operator.health_check.get_parameter("port"), timeout=5
        )

        # During execution, the server must be responsive
        conn.request("GET", "/check")
        resp = conn.getresponse()

        self.assertEqual(resp.status, 200)
        body = resp.read().decode("utf-8")
        self.assertIn("OK", body)

        executor_thread.join()

        # After execution, the server must be closed
        with self.assertRaises(ConnectionRefusedError):
            conn.request("GET", "/check")

        conn.close()
