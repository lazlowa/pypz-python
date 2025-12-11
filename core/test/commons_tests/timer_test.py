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
import unittest

from pypz.core.commons.utils import InterruptableTimer, current_time_millis


class TimerTest(unittest.TestCase):

    def test_timer_without_interrupt(self):
        timer = InterruptableTimer()

        start_time = current_time_millis()
        timer.sleep(0.1)
        self.assertFalse(100 > current_time_millis() - start_time)

        start_time = current_time_millis()
        timer.sleep(0.5)
        self.assertFalse(500 > current_time_millis() - start_time)

        start_time = current_time_millis()
        timer.sleep(1)
        self.assertFalse(1000 > current_time_millis() - start_time)

    def test_timer_with_interrupt(self):
        timer = InterruptableTimer()
        timer.interrupt()

        start_time = current_time_millis()
        timer.sleep(0.1)
        self.assertTrue(100 > current_time_millis() - start_time)

        start_time = current_time_millis()
        timer.sleep(0.5)
        self.assertTrue(500 > current_time_millis() - start_time)

        start_time = current_time_millis()
        timer.sleep(1)
        self.assertTrue(1000 > current_time_millis() - start_time)
