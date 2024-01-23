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
from enum import Enum

from pypz.core.commons.parameters import ParameterSchemaBuilder


class TestEnum(Enum):
    RED = 1
    GREEN = 2
    BLUE = 3


class ConfigurationSchemaBuilderTest(unittest.TestCase):

    def test_add_optional_parameters_expect_success(self):
        builder = ParameterSchemaBuilder.builder()

        builder.new_parameter("paramInt").as_int().optional()
        builder.new_parameter("paramBoolean").as_boolean().optional()
        builder.new_parameter("paramByte").as_byte().optional()
        builder.new_parameter("paramFloat").as_float().optional()
        builder.new_parameter("paramMap").as_map({"nestedKey": "nestedVal"}).optional()
        builder.new_parameter("paramMapOf").as_map_of(str, object).optional()
        builder.new_parameter("paramList").as_list().optional()
        builder.new_parameter("paramSet").as_set().optional()
        builder.new_parameter("paramListOf").as_list_of(int).optional()
        builder.new_parameter("paramSetOf").as_set_of(int).optional()
        builder.new_parameter("paramString").as_string().optional()
        builder.new_parameter("paramObject").as_object(ConfigurationSchemaBuilderTest).optional()
        builder.new_parameter("paramEnum").as_enum(TestEnum).optional()
        builder.new_parameter("customType").as_custom_type("111").optional()

        schema = builder.build()

        self.assertEqual(14, len(schema))
        self.assertIsNone(schema.get(ParameterSchemaBuilder.RequiredFieldName))
        self.assertEqual(schema["paramInt"], ParameterSchemaBuilder.IntTypeName)
        self.assertEqual(schema["paramBoolean"], ParameterSchemaBuilder.BooleanTypeName)
        self.assertEqual(schema["paramByte"], ParameterSchemaBuilder.ByteTypeName)
        self.assertEqual(schema["paramFloat"], ParameterSchemaBuilder.FloatTypeName)
        self.assertEqual(schema["paramList"], ParameterSchemaBuilder.ListTypeName)
        self.assertEqual(schema["paramSet"], ParameterSchemaBuilder.SetTypeName)
        self.assertEqual(schema["paramListOf"], f"{ParameterSchemaBuilder.ListTypeName}<{ParameterSchemaBuilder.IntTypeName}>")
        self.assertEqual(schema["paramSetOf"], f"{ParameterSchemaBuilder.SetTypeName}<{ParameterSchemaBuilder.IntTypeName}>")
        self.assertEqual(schema["paramMapOf"], f"{ParameterSchemaBuilder.MapTypeName}<{ParameterSchemaBuilder.StringTypeName},{ParameterSchemaBuilder.ObjectTypeName}>")
        self.assertEqual(schema["paramString"], ParameterSchemaBuilder.StringTypeName)
        self.assertEqual(schema["paramObject"], "ConfigurationSchemaBuilderTest")
        self.assertEqual(schema["paramEnum"], ['RED', 'GREEN', 'BLUE'])
        self.assertEqual(schema["paramMap"], {"nestedKey": "nestedVal"})
        self.assertEqual(schema["customType"], "111")

    def test_add_required_parameters_expect_success(self):
        builder = ParameterSchemaBuilder.builder()

        builder.new_parameter("paramInt").as_int().required()
        builder.new_parameter("paramBoolean").as_boolean().required()
        builder.new_parameter("paramByte").as_byte().required()
        builder.new_parameter("paramCustom").as_custom_type("customType").required()
        builder.new_parameter("paramFloat").as_float().required()
        builder.new_parameter("paramMap").as_map({"nestedKey": "nestedVal"}).required()
        builder.new_parameter("paramList").as_list_of(int).required()
        builder.new_parameter("paramSet").as_set_of(int).required()
        builder.new_parameter("paramString").as_string().required()
        builder.new_parameter("paramObject").as_object(str).required()
        builder.new_parameter("paramEnum").as_enum(TestEnum).required()

        schema = builder.build()
        required = schema.get(ParameterSchemaBuilder.RequiredFieldName)

        self.assertEqual(12, len(schema))
        self.assertEqual(11, len(required))
        self.assertTrue("paramBoolean" in required)
        self.assertTrue("paramByte" in required)
        self.assertTrue("paramFloat" in required)
        self.assertTrue("paramList" in required)
        self.assertTrue("paramSet" in required)
        self.assertTrue("paramString" in required)
        self.assertTrue("paramObject" in required)
        self.assertTrue("paramEnum" in required)
        self.assertTrue("paramMap" in required)
        self.assertTrue("paramInt" in required)
        self.assertTrue("paramCustom" in required)

    def test_multiple_parameter_registration_with_same_name_expect_fail(self):
        builder = ParameterSchemaBuilder.builder()

        builder.new_parameter("paramInt").as_int().required()

        with self.assertRaises(AttributeError):
            builder.new_parameter("paramInt").as_int().required()
