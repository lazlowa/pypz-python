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

from pypz.core.commons.utils import is_type_allowed
from pypz.core.specs.instance import Instance
from pypz.core.specs.operator import Operator
from pypz.core.specs.plugin import InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin, Plugin, \
    PortPlugin
from pypz.core.specs.utils import remove_super_classes, load_class_by_name


class UtilsTest(unittest.TestCase):
    def test_load_class_by_name_with_valid_class_name(self):
        cls = load_class_by_name("pypz.core.specs.instance.Instance")
        self.assertEqual(cls, Instance)

    def test_load_class_by_name_with_valid_nested_class_name(self):
        cls = load_class_by_name("pypz.core.specs.operator.Operator.Logger")
        self.assertEqual(cls, Operator.Logger)

    def test_load_class_by_name_with_invalid_class_name_expect_error(self):
        with self.assertRaises(ValueError):
            load_class_by_name("Logger")

    def test_load_class_by_name_with_invalid_class_type_expect_error(self):
        with self.assertRaises(TypeError):
            load_class_by_name("pypz.core.specs.instance")

    def test_load_class_by_name_with_non_existent_module(self):
        with self.assertRaises(ModuleNotFoundError):
            load_class_by_name("nonexistent.DummyClass")

    def test_load_class_by_name_with_non_existent_class(self):
        with self.assertRaises(AttributeError):
            load_class_by_name("pypz.DummyClass")

    def test_remove_super_classes_cases(self):
        self.assertEqual({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin},
                         remove_super_classes({Instance, Plugin, PortPlugin, InputPortPlugin, OutputPortPlugin,
                                               ResourceHandlerPlugin, ServicePlugin}))

        self.assertEqual({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin},
                         remove_super_classes({Plugin, PortPlugin, InputPortPlugin, OutputPortPlugin,
                                               ResourceHandlerPlugin, ServicePlugin, Instance}))

        self.assertEqual({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin},
                         remove_super_classes({PortPlugin, InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin,
                                               ServicePlugin, Instance, Plugin}))

        self.assertEqual({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin},
                         remove_super_classes({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin,
                                               ServicePlugin, Instance, Plugin, PortPlugin}))

        self.assertEqual({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin},
                         remove_super_classes({OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin, Instance,
                                               Plugin, PortPlugin, InputPortPlugin}))

        self.assertEqual({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin},
                         remove_super_classes({ResourceHandlerPlugin, ServicePlugin, Instance, Plugin, PortPlugin,
                                               InputPortPlugin, OutputPortPlugin}))

        self.assertEqual({InputPortPlugin, OutputPortPlugin, ResourceHandlerPlugin, ServicePlugin},
                         remove_super_classes({ServicePlugin, Instance, Plugin, PortPlugin, InputPortPlugin,
                                               OutputPortPlugin, ResourceHandlerPlugin}))

    def test_is_type_allowed_with_simple_types(self):
        self.assertTrue(is_type_allowed("string", (str, int, float)))
        self.assertTrue(is_type_allowed(1, (str, int, float)))
        self.assertTrue(is_type_allowed(1.0, (str, int, float)))

        self.assertFalse(is_type_allowed("string", (type(None),)))
        self.assertFalse(is_type_allowed(1, (type(None),)))
        self.assertFalse(is_type_allowed(1.0, (type(None),)))

        self.assertFalse(is_type_allowed(dict(), (type(None),)))
        self.assertFalse(is_type_allowed(list(), (type(None),)))
        self.assertFalse(is_type_allowed(set(), (type(None),)))

    def test_is_type_allowed_with_complex_types_list(self):
        self.assertTrue(is_type_allowed("string", (str, list)))
        self.assertTrue(is_type_allowed(["string"], (str, list)))
        self.assertTrue(is_type_allowed(["string", 0.0], (str, list, float)))

        self.assertFalse(is_type_allowed(["string", 0], (str, list)))
        self.assertFalse(is_type_allowed({"string"}, (str, list)))
        self.assertFalse(is_type_allowed([{"string"}], (str, list)))
        self.assertFalse(is_type_allowed({"0": "string"}, (str, list)))
        self.assertFalse(is_type_allowed([{"0": "string"}], (str, list)))

    def test_is_type_allowed_with_complex_types_set(self):
        self.assertTrue(is_type_allowed(0, (int, set)))
        self.assertTrue(is_type_allowed({0, 1, 2}, (int, set)))
        self.assertTrue(is_type_allowed({0, 0.0}, (int, set, float)))

        self.assertFalse(is_type_allowed({0, "string"}, (int, set)))
        self.assertFalse(is_type_allowed([0, 1, 2], (int, set)))

    def test_is_type_allowed_with_complex_types_dict(self):
        self.assertTrue(is_type_allowed(0.0, (float, dict)))
        self.assertTrue(is_type_allowed({"0.0": 0.0}, (float, dict)))
        self.assertTrue(is_type_allowed({"0.0": 0.0, "0": 0}, (int, dict, float)))
        self.assertTrue(is_type_allowed({"0": [0.0, 1.0]}, (float, dict, list)))
        self.assertTrue(is_type_allowed({"0": {0.0, 1.0}}, (float, dict, set)))
        self.assertTrue(is_type_allowed({"0": {"0": 0.0, "1": 1.0}}, (float, dict)))
        self.assertTrue(is_type_allowed({"0": {"0": [0.0], "1": {1.0}}}, (float, dict, list, set)))

        self.assertFalse(is_type_allowed({"0": {"0": [0.0], "1": {1.0}}}, (float, dict, list)))
        self.assertFalse(is_type_allowed({"0": {"0": [0.0], "1": {1.0}}}, (float, dict, set)))
        self.assertFalse(is_type_allowed({"0": {"0": [0.0], "1": {1}}}, (float, dict, list, set)))
        self.assertFalse(is_type_allowed({"0": {"0": [0.0], "1": {"1"}}}, (float, dict, list, set)))
        self.assertFalse(is_type_allowed(0, (float, dict)))
        self.assertFalse(is_type_allowed({"0": 0}, (float, dict)))
        self.assertFalse(is_type_allowed({"0": [0.0, 0]}, (float, dict)))

    def test_is_type_allowed_with_complex_types_mixed(self):
        self.assertTrue(is_type_allowed(0, (str, int, float, set, list, dict)))
        self.assertTrue(is_type_allowed(0.0, (str, int, float, set, list, dict)))
        self.assertTrue(is_type_allowed("0", (str, int, float, set, list, dict)))

        self.assertTrue(is_type_allowed([0, 0.0, "0"], (str, int, float, set, list, dict)))
        self.assertTrue(is_type_allowed({0, 0.0, "0"}, (str, int, float, set, list, dict)))
        self.assertTrue(is_type_allowed({"a": 0, "b": 0.0, "c": "0"}, (str, int, float, set, list, dict)))

        self.assertTrue(is_type_allowed([{0, 0.0, "0"}], (str, int, float, set, list, dict)))
        self.assertTrue(is_type_allowed({"a": [0, 0.0, "0"], "b": {0, 0.0, "0"}, "c": {"a": 0, "b": 0.0, "c": "0"}},
                                        (str, int, float, set, list, dict)))
        self.assertTrue(is_type_allowed({"c": {"a": {0, 0.0, "0"}, "b": [0, 0.0, "0"], "c": "0"}},
                                        (str, int, float, set, list, dict)))
        self.assertTrue(is_type_allowed([{"c": {"a": {0, 0.0, "0"}, "b": [0, 0.0, "0"], "c": "0"}}],
                                        (str, int, float, set, list, dict)))

        self.assertFalse(is_type_allowed((0, 0.0, "0"), (str, int, float, set, list, dict)))

        self.assertFalse(is_type_allowed([(0, 0.0, "0"),], (str, int, float, set, list, dict)))
        self.assertFalse(is_type_allowed({"a": [0, 0.0, "0"], "b": (0, 0.0, "0"), "c": {"a": 0, "b": 0.0, "c": "0"}},
                                         (str, int, float, set, list, dict)))
        self.assertFalse(is_type_allowed({"c": {"a": {0, 0.0, "0"}, "b": (0, 0.0, "0"), "c": "0"}},
                                         (str, int, float, set, list, dict)))
        self.assertFalse(is_type_allowed([{"c": {"a": {0, 0.0, "0"}, "b": (0, 0.0, "0"), "c": "0"}}],
                                         (str, int, float, set, list, dict)))
