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
import os
from pypz.core.commons.utils import TemplateResolver


class TemplateResolverTest(unittest.TestCase):

    def test_env_var_resolver_with_strings_expect_success(self):
        os.environ["PPFW_TEST"] = "testValue"
        os.environ["PPFW_TEST2"] = "testValue2"

        test_simple1_s = "${env:PPFW_TEST}"
        test_simple2_s = "test${env:PPFW_TEST}"
        test_simple3_s = "${env:PPFW_TEST}test"
        test_complex1_s = "test${env:PPFW_TEST}test${env:PPFW_TEST}test"
        test_complex2_s = "test${env:PPFW_TEST}test${env:PPFW_TEST2}test"
        test_complex3_s = "test$(env:PPFW_TEST)test$(env:PPFW_TEST)test"
        test_complex4_s = "test$(env:PPFW_TEST)test${env:PPFW_TEST}test"

        template_resolver1 = TemplateResolver("${", "}")
        template_resolver2 = TemplateResolver("$(", ")")

        resolved_simple1 = template_resolver1.resolve(test_simple1_s)
        resolved_simple2 = template_resolver1.resolve(test_simple2_s)
        resolved_simple3 = template_resolver1.resolve(test_simple3_s)
        resolved_complex1 = template_resolver1.resolve(test_complex1_s)
        resolved_complex2 = template_resolver1.resolve(test_complex2_s)
        resolved_complex3 = template_resolver2.resolve(test_complex3_s)
        resolved_complex4 = template_resolver2.resolve(test_complex4_s)

        self.assertEqual(os.getenv("PPFW_TEST"), resolved_simple1)
        self.assertEqual("test" + os.getenv("PPFW_TEST"), resolved_simple2)
        self.assertEqual(os.getenv("PPFW_TEST") + "test", resolved_simple3)
        self.assertEqual("test" + os.getenv("PPFW_TEST") + "test" + os.getenv("PPFW_TEST") + "test", resolved_complex1)
        self.assertEqual("test" + os.getenv("PPFW_TEST") + "test" + os.getenv("PPFW_TEST2") + "test", resolved_complex2)

        self.assertEqual("test" + os.getenv("PPFW_TEST") + "test" + os.getenv("PPFW_TEST") + "test", resolved_complex3)
        self.assertEqual("test" + os.getenv("PPFW_TEST") + "test${env:PPFW_TEST}test", resolved_complex4)

    def test_env_var_resolver_with_invalid_strings_expect_error(self):
        os.environ["PPFW_TEST"] = "testValue"

        invalid1_s = "${PPFW_TEST}"
        invalid2_s = "${}"
        invalid3_s = "${env:}"
        invalid4_s = "${en:PPFW_TEST}"
        invalid5_s = "${env:PPFW_SURE_IT_IS_NOT_EXISTING}"
        invalid6_s = "${env:PPFW_TEST}${PPFW_TEST}"
        invalid7_s = "${env:PPFW_TEST"
        invalid8_s = "env:PPFW_TEST}"

        template_resolver = TemplateResolver("${", "}")

        self.assertEqual("", template_resolver.resolve(invalid1_s))
        self.assertEqual("", template_resolver.resolve(invalid2_s))
        self.assertEqual("", template_resolver.resolve(invalid3_s))
        self.assertEqual("", template_resolver.resolve(invalid4_s))
        self.assertEqual("", template_resolver.resolve(invalid5_s))
        self.assertEqual(os.environ["PPFW_TEST"], template_resolver.resolve(invalid6_s))

        self.assertEqual(invalid7_s, template_resolver.resolve(invalid7_s))
        self.assertEqual(invalid8_s, template_resolver.resolve(invalid8_s))

    def test_env_var_resolver_with_map_expect_success(self):
        os.environ["PPFW_TEST"] = "testValue"

        ref_map = dict()
        ref_map["key1"] = "${env:PPFW_TEST}"
        ref_map["key2"] = "env:PPFW_TEST"

        template_resolver = TemplateResolver("${", "}")

        resolved_map = template_resolver.resolve(ref_map)

        self.assertEqual(os.getenv("PPFW_TEST"), resolved_map["key1"])
        self.assertEqual("env:PPFW_TEST", resolved_map["key2"])
        self.assertNotEqual(ref_map, resolved_map)

    def test_env_var_resolver_with_collections_expect_success(self):
        os.environ["PPFW_TEST"] = "testValue"

        ref_list = list()
        ref_list.append("${env:PPFW_TEST}")
        ref_list.append("env:PPFW_TEST")

        ref_set = set()
        ref_set.add("${env:PPFW_TEST}")
        ref_set.add("env:PPFW_TEST")

        template_resolver = TemplateResolver("${", "}")

        resolved_list = template_resolver.resolve(ref_list)
        resolved_set = template_resolver.resolve(ref_set)

        self.assertTrue(os.getenv("PPFW_TEST") in resolved_list)
        self.assertTrue("env:PPFW_TEST" in resolved_list)
        self.assertTrue(isinstance(resolved_list, list))
        self.assertEqual(2, len(resolved_list))
        self.assertNotEqual(ref_list, resolved_list)

        self.assertTrue(os.getenv("PPFW_TEST") in resolved_set)
        self.assertTrue("env:PPFW_TEST" in resolved_set)
        self.assertTrue(isinstance(resolved_set, set))
        self.assertEqual(2, len(resolved_set))
        self.assertNotEqual(ref_list, resolved_set)

    def test_env_var_resolver_with_complex_structure_expect_success(self):
        os.environ["PPFW_TEST"] = "testValue"

        ref_set = set()
        ref_set.add("${env:PPFW_TEST}")
        ref_set.add("env:PPFW_TEST")

        reference_map = dict()
        reference_map["key1"] = "${env:PPFW_TEST}"
        reference_map["key2"] = "env:PPFW_TEST"
        reference_map["key3"] = ref_set

        ref_list = list()
        ref_list.append(reference_map)

        template_resolver = TemplateResolver("${", "}")

        resolved_list = template_resolver.resolve(ref_list)

        self.assertEqual(1, len(resolved_list))
        self.assertTrue(isinstance(resolved_list[0], dict))
        self.assertEqual(3, len(resolved_list[0]))
        self.assertEqual(os.getenv("PPFW_TEST"), resolved_list[0]["key1"])
        self.assertEqual("env:PPFW_TEST", resolved_list[0]["key2"])
        self.assertTrue(isinstance(resolved_list[0]["key3"], set))
        self.assertEqual(2, len(resolved_list[0]["key3"]))
        self.assertTrue(os.getenv("PPFW_TEST") in resolved_list[0]["key3"])
        self.assertTrue("env:PPFW_TEST" in resolved_list[0]["key3"])
