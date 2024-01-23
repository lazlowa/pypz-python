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

from pypz.core.commons.parameters import retrieve_parameters, RequiredParameter, ExpectedParameter
from pypz.core.specs.misc import BlankInstance


# Test resources
# ==============


class TestObjectClass(BlankInstance[None]):
    pass


class BaseTestClass(BlankInstance[None]):
    req_int = RequiredParameter(int)
    _req_int = RequiredParameter(int)

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.req_int = None
        self._req_int = None
        self.__req_int = None


class InheritedTestClass(BaseTestClass):

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)


class InheritedTestClassWithReimplementedParameter(BaseTestClass):
    _req_int = RequiredParameter(int)

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self._req_int = None


class AllTypeTestClass(BlankInstance[None]):
    req_int = RequiredParameter(int)
    req_str = RequiredParameter(str)
    req_bool = RequiredParameter(bool)
    req_set = RequiredParameter(set)
    req_list = RequiredParameter(list)
    req_dict = RequiredParameter(dict)
    req_set_of_str = RequiredParameter(set[str])
    req_list_of_str = RequiredParameter(list[str])
    req_dict_of_str_obj = RequiredParameter(dict[str, object])

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.req_int = None
        self.req_str = None
        self.req_bool = None
        self.req_set = None
        self.req_list = None
        self.req_dict = None
        self.req_set_of_str = None
        self.req_list_of_str = None
        self.req_dict_of_str_obj = None


class TestClassWithIdenticalParameterNames(BlankInstance[None]):
    one_name = RequiredParameter(int)
    other_name = RequiredParameter(int, alt_name="one_name")

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.one_name = None
        self.other_name = None


def on_value_change(instance, value):
    if value is not None:
        instance.value = value


class TestClassWithOnChangeCallback(BlankInstance[None]):
    param = RequiredParameter(int, on_update=on_value_change)

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.param = None
        self.value = 0

# Tests
# =====


class ParameterTest(unittest.TestCase):

    # Basic retrieval tests
    # =====================

    def test_parameter_retrieval_with_inheritance(self):
        base = BaseTestClass("instance")
        inh = InheritedTestClass("instance")

        base_parameters = retrieve_parameters(base, ExpectedParameter)
        inh_parameters = retrieve_parameters(inh, ExpectedParameter)

        self.assertEqual(2, len(base_parameters))
        self.assertTrue("req_int" in base_parameters)
        self.assertTrue("_req_int" in base_parameters)
        self.assertFalse("_BaseTestClass__req_int" in base_parameters)

        self.assertEqual(2, len(inh_parameters))
        self.assertTrue("req_int" in inh_parameters)
        self.assertTrue("_req_int" in inh_parameters)
        self.assertFalse("_BaseTestClass__req_int" in inh_parameters)
        self.assertFalse("_InheritedTestClass__req_int" in inh_parameters)

    def test_parameter_retrieval_with_redefined_parameter(self):
        with self.assertRaises(AttributeError):
            InheritedTestClassWithReimplementedParameter("instance")

    def test_parameter_injection(self):
        inh = InheritedTestClass("instance")
        inh_parameters = retrieve_parameters(inh, ExpectedParameter)

        for param_name, param_type in inh_parameters.items():
            setattr(inh, param_name, 1234)

        self.assertEqual(1234, inh.req_int)
        self.assertEqual(1234, inh._req_int)

    def test_parameter_injection_with_invalid_type(self):
        inh = InheritedTestClass("instance")
        inh_parameters = retrieve_parameters(inh, ExpectedParameter)

        for param_name, param_type in inh_parameters.items():
            with self.assertRaises(TypeError):
                setattr(inh, param_name, "1234")

    # ExpectedParameter type tests
    # ====================

    def test_parameter_types(self):
        test_obj = AllTypeTestClass("instance")

        params = retrieve_parameters(test_obj, ExpectedParameter)

        self.assertEqual(int, params["req_int"].parameter_type)
        self.assertEqual(str, params["req_str"].parameter_type)
        self.assertEqual(bool, params["req_bool"].parameter_type)
        self.assertEqual(set, params["req_set"].parameter_type)
        self.assertEqual(list, params["req_list"].parameter_type)
        self.assertEqual(dict, params["req_dict"].parameter_type)
        self.assertEqual(set, params["req_set_of_str"].parameter_type)
        self.assertEqual(str, params["req_set_of_str"].generic_types[0])
        self.assertEqual(list, params["req_list_of_str"].parameter_type)
        self.assertEqual(str, params["req_list_of_str"].generic_types[0])
        self.assertEqual(dict, params["req_dict_of_str_obj"].parameter_type)
        self.assertEqual(str, params["req_dict_of_str_obj"].generic_types[0])
        self.assertEqual(object, params["req_dict_of_str_obj"].generic_types[1])

    def test_parameter_assignment_with_valid_types(self):
        test_obj = AllTypeTestClass("instance")

        setattr(test_obj, "req_int", 1234)
        setattr(test_obj, "req_str", "1234")
        setattr(test_obj, "req_bool", True)
        setattr(test_obj, "req_set", {"1", "2", "3"})
        setattr(test_obj, "req_list", ["1", "2", "3"])
        setattr(test_obj, "req_set_of_str", {"1", "2", "3"})
        setattr(test_obj, "req_list_of_str", ["1", "2", "3"])

        self.assertEqual(1234, test_obj.req_int)
        self.assertEqual("1234", test_obj.req_str)
        self.assertEqual(True, test_obj.req_bool)
        self.assertEqual({"1", "2", "3"}, test_obj.req_set)
        self.assertEqual(["1", "2", "3"], test_obj.req_list)
        self.assertEqual({"1", "2", "3"}, test_obj.req_set_of_str)
        self.assertEqual(["1", "2", "3"], test_obj.req_list_of_str)

        setattr(test_obj, "req_any", 1234)
        self.assertEqual(1234, test_obj.req_any)
        setattr(test_obj, "req_any", "1234")
        self.assertEqual("1234", test_obj.req_any)
        setattr(test_obj, "req_any", {"1", "2", "3"})
        self.assertEqual({"1", "2", "3"}, test_obj.req_any)
        setattr(test_obj, "req_any", ["1", "2", "3"])
        self.assertEqual(["1", "2", "3"], test_obj.req_any)

    def test_parameter_assignment_with_invalid_types(self):
        test_obj = AllTypeTestClass("instance")

        with self.assertRaises(TypeError):
            setattr(test_obj, "req_int", "1234")
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_str", 1234)
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_bool", "True")
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_set", ["1", "2", "3"])
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_list", {"1", "2", "3"})
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_dict", ["1", "2", "3"])
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_dict", {"1": object()})
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_set_of_str", [1, 2, 3])
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_list_of_str", {1, 2, 3})
        with self.assertRaises(TypeError):
            setattr(test_obj, "req_dict_of_str_obj", "dict_ref")

    def test_identical_parameter_names_expect_error(self):
        with self.assertRaises(AttributeError):
            TestClassWithIdenticalParameterNames("instance")

    def test_parameter_direct_setting_with_on_value_changed_callback(self):
        test_obj_1 = TestClassWithOnChangeCallback("instance")
        test_obj_2 = TestClassWithOnChangeCallback("instance")
        test_obj_1.param = 10
        test_obj_2.param = 20

        self.assertEqual(10, test_obj_1.value)
        self.assertEqual(20, test_obj_2.value)

    def test_parameter_indirect_setting_with_on_value_changed_callback(self):
        test_obj_1 = TestClassWithOnChangeCallback("instance")
        test_obj_2 = TestClassWithOnChangeCallback("instance")
        test_obj_1.set_parameter("param", 10)
        test_obj_2.set_parameter("param", 20)

        self.assertEqual(10, test_obj_1.value)
        self.assertEqual(20, test_obj_2.value)
