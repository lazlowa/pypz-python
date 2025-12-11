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
import os
import types
from unittest import TestCase

from pypz.core.specs.dtos import InstanceDTO, SpecDTO
from pypz.core.specs.instance import Instance
from pypz.core.specs.misc import BlankInstance
from pypz.core.specs.plugin import InputPortPlugin, ResourceHandlerPlugin
from pypz.core.specs.utils import resolve_dependency_graph

from core.test.specs_tests.instance_test_resources import (
    TestClassForDependencyResolution,
    TestClassL0,
    TestClassL3,
    TestClassWithDifferentNestedType,
)


class InstanceTest(TestCase):

    def test_simple_name_resolution(self):
        l0 = TestClassL0("l0")

        self.assertEqual("l0", l0.get_simple_name())
        self.assertEqual("l10", l0.l10.get_simple_name())
        self.assertEqual("other_name", l0.l11.get_simple_name())

        self.assertEqual("l2", l0.l10.l2.get_simple_name())
        self.assertEqual("l2", l0.l11.l2.get_simple_name())

        self.assertEqual("l3", l0.l10.l2.l3.get_simple_name())
        self.assertEqual("l3", l0.l11.l2.l3.get_simple_name())

        with self.assertRaises(AttributeError):
            TestClassL0()

    def test_full_name_resolution(self):
        l0 = TestClassL0("l0")

        self.assertEqual("l0", l0.get_full_name())
        self.assertEqual("l0.l10", l0.l10.get_full_name())
        self.assertEqual("l0.other_name", l0.l11.get_full_name())
        self.assertEqual("l0.l10.l2", l0.l10.l2.get_full_name())
        self.assertEqual("l0.other_name.l2", l0.l11.l2.get_full_name())
        self.assertEqual("l0.l10.l2.l3", l0.l10.l2.l3.get_full_name())
        self.assertEqual("l0.other_name.l2.l3", l0.l11.l2.l3.get_full_name())

    def test_object_linking_with_nested_objects(self):
        l0 = TestClassL0("l0")

        self.assertEqual(2, len(l0.get_protected().get_nested_instances()))
        self.assertEqual(l0.l10, l0.get_protected().get_nested_instance("l10"))
        self.assertEqual(l0.l11, l0.get_protected().get_nested_instance("other_name"))
        self.assertEqual(l0, l0.l10.get_context())
        self.assertEqual(l0, l0.l11.get_context())

        self.assertEqual(1, len(l0.l10.get_protected().get_nested_instances()))
        self.assertEqual(1, len(l0.l11.get_protected().get_nested_instances()))
        self.assertEqual(l0.l10.l2, l0.l10.get_protected().get_nested_instance("l2"))
        self.assertEqual(l0.l11.l2, l0.l11.get_protected().get_nested_instance("l2"))
        self.assertEqual(l0.l10, l0.l10.l2.get_context())
        self.assertEqual(l0.l11, l0.l11.l2.get_context())

        self.assertEqual(1, len(l0.l10.l2.get_protected().get_nested_instances()))
        self.assertEqual(1, len(l0.l11.l2.get_protected().get_nested_instances()))
        self.assertEqual(
            l0.l10.l2.l3, l0.l10.l2.get_protected().get_nested_instance("l3")
        )
        self.assertEqual(
            l0.l11.l2.l3, l0.l11.l2.get_protected().get_nested_instance("l3")
        )
        self.assertEqual(l0.l10.l2, l0.l10.l2.l3.get_context())
        self.assertEqual(l0.l11.l2, l0.l11.l2.l3.get_context())

        self.assertEqual(0, len(l0.l10.l2.l3.get_protected().get_nested_instances()))
        self.assertEqual(0, len(l0.l11.l2.l3.get_protected().get_nested_instances()))
        self.assertNotEqual(l0.l10.l2.l3, l0.l10.l2.l3.l4.get_context())
        self.assertNotEqual(l0.l11.l2.l3, l0.l11.l2.l3.l4.get_context())

        l0 = TestClassWithDifferentNestedType("l0")

        self.assertEqual(3, len(l0.get_protected().get_nested_instances()))
        self.assertEqual(l0.a, l0.get_protected().get_nested_instance("a"))
        self.assertEqual(l0.b, l0.get_protected().get_nested_instance("b"))
        self.assertEqual(l0.c, l0.get_protected().get_nested_instance("c"))
        self.assertEqual(l0, l0.a.get_context())
        self.assertEqual(l0, l0.b.get_context())
        self.assertEqual(l0, l0.c.get_context())

    def test_depends_on_handling(self):
        l0 = TestClassL0("l0")

        self.assertTrue(l0.l11 in l0.l10.get_protected().get_depends_on())

        # Circular dependency
        with self.assertRaises(RecursionError):
            l0.l11.depends_on(l0.l10)

        # Self dependency
        with self.assertRaises(AttributeError):
            l0.depends_on(l0)

        # Dependency with from different context
        with self.assertRaises(AttributeError):
            l0.l10.l2.depends_on(l0.l11.l2)

        # Invalid dependency type
        with self.assertRaises(TypeError):
            l0.depends_on("invalid_dependency_type")

    def test_dependency_graph_resolution_case_1(self):
        instance = TestClassForDependencyResolution("base")
        instance.instance_1.depends_on(instance.instance_0)
        instance.instance_2.depends_on(instance.instance_1)
        instance.instance_3.depends_on(instance.instance_2)
        instance.instance_4.depends_on(instance.instance_3)

        dependency_levels = resolve_dependency_graph(
            instance.get_protected().get_nested_instances().values()
        )

        self.assertEqual(5, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[0]))
        self.assertEqual(1, len(dependency_levels[1]))
        self.assertEqual(1, len(dependency_levels[2]))
        self.assertEqual(1, len(dependency_levels[3]))
        self.assertEqual(1, len(dependency_levels[4]))
        self.assertTrue(instance.instance_0 in dependency_levels[0])
        self.assertTrue(instance.instance_1 in dependency_levels[1])
        self.assertTrue(instance.instance_2 in dependency_levels[2])
        self.assertTrue(instance.instance_3 in dependency_levels[3])
        self.assertTrue(instance.instance_4 in dependency_levels[4])

    def test_dependency_graph_resolution_case_2(self):
        instance = TestClassForDependencyResolution("base")
        instance.instance_0.depends_on(instance.instance_1)
        instance.instance_1.depends_on(instance.instance_2)
        instance.instance_2.depends_on(instance.instance_3)
        instance.instance_3.depends_on(instance.instance_4)

        dependency_levels = resolve_dependency_graph(
            instance.get_protected().get_nested_instances().values()
        )

        self.assertEqual(5, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[0]))
        self.assertEqual(1, len(dependency_levels[1]))
        self.assertEqual(1, len(dependency_levels[2]))
        self.assertEqual(1, len(dependency_levels[3]))
        self.assertEqual(1, len(dependency_levels[4]))
        self.assertTrue(instance.instance_0 in dependency_levels[4])
        self.assertTrue(instance.instance_1 in dependency_levels[3])
        self.assertTrue(instance.instance_2 in dependency_levels[2])
        self.assertTrue(instance.instance_3 in dependency_levels[1])
        self.assertTrue(instance.instance_4 in dependency_levels[0])

    def test_dependency_graph_resolution_case_3(self):
        instance = TestClassForDependencyResolution("base")
        instance.instance_1.depends_on(instance.instance_0)
        instance.instance_2.depends_on(instance.instance_0)
        instance.instance_2.depends_on(instance.instance_1)
        instance.instance_3.depends_on(instance.instance_1)
        instance.instance_3.depends_on(instance.instance_2)
        instance.instance_4.depends_on(instance.instance_0)

        dependency_levels = resolve_dependency_graph(
            instance.get_protected().get_nested_instances().values()
        )

        self.assertEqual(4, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[0]))
        self.assertEqual(2, len(dependency_levels[1]))
        self.assertEqual(1, len(dependency_levels[2]))
        self.assertEqual(1, len(dependency_levels[3]))
        self.assertTrue(instance.instance_0 in dependency_levels[0])
        self.assertTrue(instance.instance_1 in dependency_levels[1])
        self.assertTrue(instance.instance_2 in dependency_levels[2])
        self.assertTrue(instance.instance_3 in dependency_levels[3])
        self.assertTrue(instance.instance_4 in dependency_levels[1])

    def test_dependency_graph_resolution_case_4(self):
        instance = TestClassForDependencyResolution("base")
        instance.instance_1.depends_on(instance.instance_0)
        instance.instance_2.depends_on(instance.instance_0)
        instance.instance_3.depends_on(instance.instance_0)
        instance.instance_3.depends_on(instance.instance_2)

        dependency_levels = resolve_dependency_graph(
            instance.get_protected().get_nested_instances().values()
        )

        self.assertEqual(3, len(dependency_levels))
        self.assertEqual(2, len(dependency_levels[0]))
        self.assertEqual(2, len(dependency_levels[1]))
        self.assertEqual(1, len(dependency_levels[2]))
        self.assertTrue(instance.instance_0 in dependency_levels[0])
        self.assertTrue(instance.instance_1 in dependency_levels[1])
        self.assertTrue(instance.instance_2 in dependency_levels[1])
        self.assertTrue(instance.instance_3 in dependency_levels[2])
        self.assertTrue(instance.instance_4 in dependency_levels[0])

    def test_dependency_order_resolution_with_circular_dependency_expect_error(self):
        instance = TestClassForDependencyResolution("base")
        instance.instance_4.depends_on(instance.instance_3)
        instance.instance_3.depends_on(instance.instance_2)
        instance.instance_2.depends_on(instance.instance_1)
        instance.instance_1.depends_on(instance.instance_0)

        instance.instance_0._Instance__depends_on.add(instance.instance_4)

        with self.assertRaises(RecursionError):
            resolve_dependency_graph(
                instance.get_protected().get_nested_instances().values()
            )

    def test_dependency_order_resolution_with_extra_dependency(self):
        # The case shall be tested, where one instance has an extra dependency
        # which is not in the context of the resolution. This can occur for example
        # if two plugin instances are implementing different interfaces, but
        # one depends on the other. Since those plugin instances will not be
        # present for the same type in plugin type registry, those cannot
        # be used for dependency resolution.

        instance = TestClassForDependencyResolution("base")
        instance.instance_1.depends_on(instance.instance_0)
        instance.instance_2.depends_on(instance.instance_1)
        instance.instance_3.depends_on(instance.instance_2)
        instance.instance_4.depends_on(instance.instance_3)

        instance.instance_4._Instance__depends_on.add(BlankInstance("inst"))

        dependency_levels = resolve_dependency_graph(
            instance.get_protected().get_nested_instances().values()
        )

        self.assertEqual(5, len(dependency_levels))
        self.assertEqual(1, len(dependency_levels[0]))
        self.assertEqual(1, len(dependency_levels[1]))
        self.assertEqual(1, len(dependency_levels[2]))
        self.assertEqual(1, len(dependency_levels[3]))
        self.assertEqual(1, len(dependency_levels[4]))
        self.assertTrue(instance.instance_0 in dependency_levels[0])
        self.assertTrue(instance.instance_1 in dependency_levels[1])
        self.assertTrue(instance.instance_2 in dependency_levels[2])
        self.assertTrue(instance.instance_3 in dependency_levels[3])
        self.assertTrue(instance.instance_4 in dependency_levels[4])

    def test_object_equality(self):
        l0 = TestClassL0("l0")
        self.assertEqual(l0, l0)

        l0_other = TestClassL0("l0")
        self.assertEqual(l0, l0_other)

    def test_object_inequality_on_simple_name(self):
        l0 = TestClassL0("l0")
        l0_other = TestClassL0("l0o")

        self.assertNotEqual(l0, l0_other)

        # Nested case
        l0 = TestClassL0("l0")
        setattr(l0.l10.l2.l3, f"_{Instance.__name__}__simple_name", "new_name")

        self.assertNotEqual(l0, l0_other)

    def test_object_inequality_on_depends_on(self):
        l0 = TestClassL0("l0")
        l0_other = TestClassL0("l0")
        depends_on = {l0_other}  # Mocking depends_on

        setattr(l0, f"_{Instance.__name__}__depends_on", depends_on)

        self.assertNotEqual(l0, l0_other)

        # Nested case
        l0 = TestClassL0("l0")
        setattr(l0.l10.l2.l3, f"_{Instance.__name__}__depends_on", depends_on)

        self.assertNotEqual(l0, l0_other)

    def test_object_inequality_on_parameters(self):
        l0 = TestClassL0("l0")
        l0_other = TestClassL0("l0")
        l0.set_parameter("param", "val")

        self.assertNotEqual(l0, l0_other)

        # Nested case
        l0 = TestClassL0("l0")
        l0.l10.l2.l3.set_parameter("param", "val")

        self.assertNotEqual(l0, l0_other)

    def test_inclusive_cascaded_parameters(self):
        l0 = TestClassL0("l0")
        l0.set_parameter("l0", "l0")
        l0.set_parameter("#l01", "l01")
        l0.set_parameter("##l012", "l012")
        l0.set_parameter("###l0123", "l0123")

        self.assertEqual(7, len(l0.get_protected().get_parameters()))
        self.assertEqual("l0", l0.get_parameter("l0"))
        self.assertEqual("l01", l0.get_parameter("l01"))
        self.assertEqual("l012", l0.get_parameter("l012"))
        self.assertEqual("l0123", l0.get_parameter("l0123"))

        self.assertEqual(6, len(l0.l10.get_protected().get_parameters()))
        self.assertEqual("l01", l0.l10.get_parameter("l01"))
        self.assertEqual("l012", l0.l10.get_parameter("l012"))
        self.assertEqual("l0123", l0.l10.get_parameter("l0123"))
        self.assertEqual(6, len(l0.l11.get_protected().get_parameters()))
        self.assertEqual("l01", l0.l11.get_parameter("l01"))
        self.assertEqual("l012", l0.l11.get_parameter("l012"))
        self.assertEqual("l0123", l0.l11.get_parameter("l0123"))

        self.assertEqual(5, len(l0.l10.l2.get_protected().get_parameters()))
        self.assertEqual("l012", l0.l10.l2.get_parameter("l012"))
        self.assertEqual("l0123", l0.l10.l2.get_parameter("l0123"))
        self.assertEqual(5, len(l0.l11.l2.get_protected().get_parameters()))
        self.assertEqual("l012", l0.l11.l2.get_parameter("l012"))
        self.assertEqual("l0123", l0.l11.l2.get_parameter("l0123"))

        self.assertEqual(4, len(l0.l10.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l0123", l0.l10.l2.l3.get_parameter("l0123"))
        self.assertEqual(4, len(l0.l11.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l0123", l0.l11.l2.l3.get_parameter("l0123"))

    def test_exclusive_cascaded_parameters(self):
        l0 = TestClassL0("l0")
        l0.set_parameter(">l1", "l1")
        l0.set_parameter(">>l2", "l2")
        l0.set_parameter(">>>l3", "l3")

        self.assertEqual(4, len(l0.l10.get_protected().get_parameters()))
        self.assertEqual("l1", l0.l10.get_parameter("l1"))
        self.assertEqual(4, len(l0.l11.get_protected().get_parameters()))
        self.assertEqual("l1", l0.l11.get_parameter("l1"))

        self.assertEqual(4, len(l0.l10.l2.get_protected().get_parameters()))
        self.assertEqual("l2", l0.l10.l2.get_parameter("l2"))
        self.assertEqual(4, len(l0.l11.l2.get_protected().get_parameters()))
        self.assertEqual("l2", l0.l11.l2.get_parameter("l2"))

        self.assertEqual(4, len(l0.l10.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l3", l0.l10.l2.l3.get_parameter("l3"))
        self.assertEqual(4, len(l0.l11.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l3", l0.l11.l2.l3.get_parameter("l3"))

    def test_mixed_cascaded_parameters(self):
        l0 = TestClassL0("l0")
        l0.set_parameter(">##l123", "l123")
        l0.set_parameter(">>#l23", "l23")
        l0.set_parameter("#>>l03", "l03")
        l0.set_parameter("##>l013", "l013")
        l0.set_parameter(">#>l13", "l13")

        self.assertEqual(5, len(l0.get_protected().get_parameters()))
        self.assertEqual("l03", l0.get_parameter("l03"))
        self.assertEqual("l013", l0.get_parameter("l013"))

        self.assertEqual(6, len(l0.l10.get_protected().get_parameters()))
        self.assertEqual("l123", l0.l10.get_parameter("l123"))
        self.assertEqual("l013", l0.l10.get_parameter("l013"))
        self.assertEqual("l13", l0.l10.get_parameter("l13"))
        self.assertEqual(6, len(l0.l11.get_protected().get_parameters()))
        self.assertEqual("l123", l0.l11.get_parameter("l123"))
        self.assertEqual("l013", l0.l11.get_parameter("l013"))
        self.assertEqual("l13", l0.l11.get_parameter("l13"))

        self.assertEqual(5, len(l0.l10.l2.get_protected().get_parameters()))
        self.assertEqual("l123", l0.l10.l2.get_parameter("l123"))
        self.assertEqual("l23", l0.l10.l2.get_parameter("l23"))
        self.assertEqual(5, len(l0.l11.l2.get_protected().get_parameters()))
        self.assertEqual("l123", l0.l11.l2.get_parameter("l123"))
        self.assertEqual("l23", l0.l11.l2.get_parameter("l23"))

        self.assertEqual(8, len(l0.l10.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l123", l0.l10.l2.l3.get_parameter("l123"))
        self.assertEqual("l23", l0.l10.l2.l3.get_parameter("l23"))
        self.assertEqual("l03", l0.l10.l2.l3.get_parameter("l03"))
        self.assertEqual("l013", l0.l10.l2.l3.get_parameter("l013"))
        self.assertEqual("l13", l0.l10.l2.l3.get_parameter("l13"))
        self.assertEqual(8, len(l0.l11.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l123", l0.l11.l2.l3.get_parameter("l123"))
        self.assertEqual("l23", l0.l11.l2.l3.get_parameter("l23"))
        self.assertEqual("l03", l0.l11.l2.l3.get_parameter("l03"))
        self.assertEqual("l013", l0.l11.l2.l3.get_parameter("l013"))
        self.assertEqual("l13", l0.l11.l2.l3.get_parameter("l13"))

    def test_parameter_setting_with_override(self):
        l0 = TestClassL0("l0")

        l0.l10.set_parameter("l01", "orig")
        l0.l10.set_parameter("l01", "new")

        self.assertEqual("new", l0.l10.get_parameter("l01"))

    def test_parameter_setting_cascaded_with_override(self):
        l0 = TestClassL0("l0")

        l0.l10.set_parameter("l01", "orig")
        l0.set_parameter(">l01", "new")

        self.assertEqual("new", l0.l10.get_parameter("l01"))

        l0.set_parameter("#l01", "latest")

        self.assertEqual("latest", l0.l10.get_parameter("l01"))

    def test_missing_required_parameters(self):
        l0 = TestClassL0("l0")

        self.assertEqual(7, len(l0.get_missing_required_parameters()))

        l0.set_parameter("###req_str", "val")

        self.assertEqual(0, len(l0.get_missing_required_parameters()))

    def test_expected_parameter_setting(self):
        l0 = TestClassL0("l0")
        l0.set_parameter("###req_str", "req")
        l0.set_parameter("###opt_str", "opt")
        l0.set_parameter("###optional_int", 1234)

        self.assertEqual("req", l0.req_str)
        self.assertEqual("opt", l0.opt_str)
        self.assertEqual(1234, l0.opt_int)

        self.assertEqual("req", l0.l10.req_str)
        self.assertEqual("opt", l0.l10.opt_str)
        self.assertEqual(1234, l0.l10.opt_int)
        self.assertEqual("req", l0.l11.req_str)
        self.assertEqual("opt", l0.l11.opt_str)
        self.assertEqual(1234, l0.l11.opt_int)

        self.assertEqual("req", l0.l10.l2.req_str)
        self.assertEqual("opt", l0.l10.l2.opt_str)
        self.assertEqual(1234, l0.l10.l2.opt_int)
        self.assertEqual("req", l0.l11.l2.req_str)
        self.assertEqual("opt", l0.l11.l2.opt_str)
        self.assertEqual(1234, l0.l11.l2.opt_int)

        self.assertEqual("req", l0.l10.l2.l3.req_str)
        self.assertEqual("opt", l0.l10.l2.l3.opt_str)
        self.assertEqual(1234, l0.l10.l2.l3.opt_int)
        self.assertEqual("req", l0.l11.l2.l3.req_str)
        self.assertEqual("opt", l0.l11.l2.l3.opt_str)
        self.assertEqual(1234, l0.l11.l2.l3.opt_int)

    def test_parameter_template_interpretation(self):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        l0 = TestClassL0("l0")
        l0.set_parameter("###req_str", "${env:PYPZ_TEST_ENV_VAR}")

        self.assertEqual("testValue", l0.req_str)

        self.assertEqual("testValue", l0.l10.req_str)
        self.assertEqual("testValue", l0.l11.req_str)

        self.assertEqual("testValue", l0.l10.l2.req_str)
        self.assertEqual("testValue", l0.l11.l2.req_str)

        self.assertEqual("testValue", l0.l10.l2.l3.req_str)
        self.assertEqual("testValue", l0.l11.l2.l3.req_str)

    def test_instance_update_with_mismatching_name(self):
        l0 = TestClassL0("l0")

        with self.assertRaises(ValueError):
            l0.update(InstanceDTO(name="mismatching"))

    def test_instance_update_with_valid_spec_name(self):
        l0 = TestClassL0("l0")

        try:
            l0.update(
                InstanceDTO(spec=SpecDTO(name=l0.get_protected().get_spec_name()))
            )
        except AttributeError:
            self.fail()

    def test_instance_update_with_invalid_spec_name(self):
        l0 = TestClassL0("l0")

        with self.assertRaises(AttributeError):
            l0.update(InstanceDTO(spec=SpecDTO(name="invalid_name")))

    def test_instance_update_with_valid_source_types(self):
        l0 = TestClassL0("l0")

        try:
            l0.update("{}")
            l0.update({})
        except (AttributeError, TypeError):
            self.fail()

    def test_instance_update_with_invalid_source_types(self):
        l0 = TestClassL0("l0")

        with self.assertRaises(TypeError):
            l0.update(0)

    def test_instance_update_parameters(self):
        l0 = TestClassL0("l0")

        to_update = InstanceDTO(
            parameters={
                "l0": "l0",
                "#l01": "l01",
                "##l012": "l012",
                "###l0123": "l0123",
            }
        )

        l0.update(to_update)

        self.assertEqual(7, len(l0.get_protected().get_parameters()))
        self.assertEqual("l0", l0.get_parameter("l0"))
        self.assertEqual("l01", l0.get_parameter("l01"))
        self.assertEqual("l012", l0.get_parameter("l012"))
        self.assertEqual("l0123", l0.get_parameter("l0123"))

        self.assertEqual(6, len(l0.l10.get_protected().get_parameters()))
        self.assertEqual("l01", l0.get_parameter("l01"))
        self.assertEqual("l012", l0.get_parameter("l012"))
        self.assertEqual("l0123", l0.get_parameter("l0123"))
        self.assertEqual(6, len(l0.l11.get_protected().get_parameters()))
        self.assertEqual("l01", l0.get_parameter("l01"))
        self.assertEqual("l012", l0.get_parameter("l012"))
        self.assertEqual("l0123", l0.get_parameter("l0123"))

        self.assertEqual(5, len(l0.l10.l2.get_protected().get_parameters()))
        self.assertEqual("l012", l0.get_parameter("l012"))
        self.assertEqual("l0123", l0.get_parameter("l0123"))
        self.assertEqual(5, len(l0.l11.l2.get_protected().get_parameters()))
        self.assertEqual("l012", l0.get_parameter("l012"))
        self.assertEqual("l0123", l0.get_parameter("l0123"))

        self.assertEqual(4, len(l0.l10.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l0123", l0.get_parameter("l0123"))
        self.assertEqual(4, len(l0.l11.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l0123", l0.get_parameter("l0123"))

    def test_instance_update_nested_instances_with_parameters_and_dependencies(self):
        l0 = TestClassWithDifferentNestedType("base")

        to_update = InstanceDTO(
            spec=SpecDTO(
                nestedInstances=[
                    InstanceDTO(name="a", parameters={"updated_param": "value"}),
                    InstanceDTO(name="b", dependsOn=["c"]),
                ]
            )
        )

        l0.update(to_update)

        self.assertEqual("value", l0.a.get_parameter("updated_param"))
        self.assertEqual(1, len(l0.b.get_protected().get_depends_on()))
        self.assertEqual(l0.c, l0.b.get_protected().get_depends_on().pop())

    def test_instance_update_with_missing_dependency_instance(self):
        l0 = TestClassWithDifferentNestedType("base")

        to_update = InstanceDTO(
            spec=SpecDTO(nestedInstances=[InstanceDTO(name="b", dependsOn=["missing"])])
        )

        with self.assertRaises(AttributeError):
            l0.update(to_update)

    def test_instance_update_with_multilevel_nesting(self):
        l0 = TestClassL0("l0")

        to_update = InstanceDTO(
            spec=SpecDTO(
                nestedInstances=[
                    InstanceDTO(
                        name="l10",
                        parameters={"l10": "l10"},
                        spec=SpecDTO(
                            nestedInstances=[
                                InstanceDTO(
                                    name="l2",
                                    parameters={"l2": "l2"},
                                    spec=SpecDTO(
                                        nestedInstances=[
                                            InstanceDTO(
                                                name="l3", parameters={"l3": "l3"}
                                            )
                                        ]
                                    ),
                                )
                            ]
                        ),
                    )
                ]
            )
        )

        l0.update(to_update)

        self.assertEqual(4, len(l0.l10.get_protected().get_parameters()))
        self.assertEqual("l10", l0.l10.get_parameter("l10"))

        self.assertEqual(4, len(l0.l10.l2.get_protected().get_parameters()))
        self.assertEqual("l2", l0.l10.l2.get_parameter("l2"))

        self.assertEqual(4, len(l0.l10.l2.l3.get_protected().get_parameters()))
        self.assertEqual("l3", l0.l10.l2.l3.get_parameter("l3"))

    def test_instance_update_with_multilevel_cascading_parameters(self):
        """
        The closer the parameter setting is to the actual instance
        the higher precedence it has. For example, if there is 3 levels
        0,1 and 2 and a cascading parameter is set on 0 and 1 for the
        instance 2 with the same name but different value, the cascading
        parameter on level 1 shall win over level 0. However, if there
        is a direct parameter setting on level 2, then it has the highest
        precedence.
        Note that, if there is a 2x inclusive cascading parameter setting
        on level 0 and a direct parameter setting on level 1, then the
        cascading shall still "pierce" through to level 2.
        """

        l0 = TestClassL0("l0")

        to_update = InstanceDTO(
            parameters={"##param_1": "cascaded_l0", "##param_2": "cascaded_l0"},
            spec=SpecDTO(
                nestedInstances=[
                    InstanceDTO(
                        name="l10",
                        parameters={"#param_1": "cascaded_l1", "param_2": "direct"},
                    )
                ]
            ),
        )

        l0.update(to_update)

        self.assertEqual("cascaded_l0", l0.get_parameter("param_1"))
        self.assertEqual("cascaded_l0", l0.get_parameter("param_2"))

        # Direct parameter setting has the highest precedence
        self.assertEqual("cascaded_l1", l0.l10.get_parameter("param_1"))
        self.assertEqual("direct", l0.l10.get_parameter("param_2"))

        # Cascaded parameter shall "pierce" through i.e., direct parameter
        # setting shall not stop cascading
        self.assertEqual("cascaded_l1", l0.l10.l2.get_parameter("param_1"))
        self.assertEqual("cascaded_l0", l0.l11.l2.get_parameter("param_2"))

    def test_instance_retrieval_with_spec_name(self):
        l0 = TestClassL0("instance")

        to_retrieve = InstanceDTO(
            name="instance", spec=SpecDTO(name=l0.get_protected().get_spec_name())
        )

        l0_retrieved = Instance.create_from_dto(to_retrieve)

        self.assertEqual(l0, l0_retrieved)

    def test_instance_retrieval_with_missing_instance_name_expect_error(self):
        l0 = TestClassL0("instance")

        to_retrieve = InstanceDTO(spec=SpecDTO(name=l0.get_protected().get_spec_name()))

        with self.assertRaises(ValueError):
            Instance.create_from_dto(to_retrieve)

    def test_instance_retrieval_with_invalid_spec_name_expect_error(self):
        to_retrieve = InstanceDTO(
            name="instance", spec=SpecDTO(name=f"{TestClassL0.__module__}:invalid_name")
        )

        with self.assertRaises(AttributeError):
            Instance.create_from_dto(to_retrieve)

    def test_instance_retrieval_with_retrievable_nested_instance(self):
        l0 = TestClassL0("instance")

        to_retrieve = InstanceDTO(
            name="instance",
            spec=SpecDTO(
                name=l0.l10.get_protected().get_spec_name(),
                nestedInstances=[
                    InstanceDTO(
                        name="instance",
                        spec=SpecDTO(name=l0.get_protected().get_spec_name()),
                    )
                ],
            ),
        )

        l0_retrieved = Instance.create_from_dto(to_retrieve)
        self.assertTrue(l0_retrieved.get_protected().has_nested_instance("instance"))

    def test_instance_object_serde(self):
        l0 = TestClassL0("instance")
        l0_from_json = Instance.create_from_string(str(l0))

        self.assertEqual(l0, l0_from_json)

    def test_instance_object_serde_with_parameter_type_set(self):
        l0 = TestClassL0("instance")
        l0.set_parameter("set", {0, 1, 2})
        l0_from_json = Instance.create_from_string(str(l0))

        self.assertEqual(l0, l0_from_json)

    def test_instance_object_serde_with_parameter_type_list(self):
        l0 = TestClassL0("instance")
        l0.set_parameter("list", [0, 1, 2])
        l0_from_json = Instance.create_from_string(str(l0))

        self.assertEqual(l0, l0_from_json)

    def test_instance_object_serde_with_parameter_type_dict(self):
        l0 = TestClassL0("instance")
        l0.set_parameter("dict", {"a": 0, "b": 0})
        l0_from_json = Instance.create_from_string(str(l0))

        self.assertEqual(l0, l0_from_json)

    def test_instance_creation_from_modified_json_string(self):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        json_string = """
        {
          "name": "l2",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "core.test.specs_tests.instance_test_resources:TestClassL2",
            "location": null,
            "expectedParameters": {
              "req_str": {
                "type": "str",
                "required": true,
                "description": null,
                "currentValue": null
              },
              "opt_str": {
                "type": "str",
                "required": false,
                "description": null,
                "currentValue": "str"
              },
              "optional_int": {
                "type": "int",
                "required": false,
                "description": null,
                "currentValue": 1234
              }
            },
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.instance.Instance'>",
            "nestedInstances": [
              {
                "name": "l3",
                "parameters": {},
                "dependsOn": [],
                "spec": {
                  "name": "core.test.specs_tests.instance_test_resources:TestClassL3",
                  "location": null,
                  "expectedParameters": {
                    "req_str": {
                      "type": "str",
                      "required": true,
                      "description": null,
                      "currentValue": null
                    },
                    "opt_str": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": "str"
                    },
                    "optional_int": {
                      "type": "int",
                      "required": false,
                      "description": null,
                      "currentValue": 1234
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.instance.Instance'>"
                  ],
                  "nestedInstanceType": null,
                  "nestedInstances": []
                }
              }
            ]
          }
        }
        """

        new_l2 = Instance.create_from_string(json_string)

        self.assertEqual("testValue", new_l2.get_parameter("env"))
        self.assertEqual("testValue", new_l2.l3.get_parameter("env"))

    def test_instance_creation_with_non_existing_spec_module_expect_error(self):
        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>"
            ]
          }
        }
        """

        with self.assertRaises(ModuleNotFoundError):
            Instance.create_from_string(json_string)

    def test_instance_creation_with_non_existing_spec_class_expect_error(self):
        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "pypz:NotExistingClass",
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>"
            ]
          }
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string)

    def test_instance_creation_with_non_existing_spec_expect_mock(self):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>"
            ]
          }
        }
        """

        instance = Instance.create_from_string(json_string, mock_nonexistent=True)
        self.assertEqual("instance", instance.get_simple_name())
        self.assertEqual(
            "dummy.module:NotExistingClass", instance.get_protected().get_spec_name()
        )
        self.assertEqual("testValue", instance.get_parameter("env"))
        self.assertTrue(issubclass(instance.__class__, Instance))
        self.assertTrue(instance.__class__.__dict__["mocked"])
        self.assertEqual("dummy.module", instance.__class__.__dict__["__module__"])
        self.assertTrue(
            isinstance(instance.__class__.__dict__["_on_interrupt"], types.FunctionType)
        )
        self.assertTrue(
            isinstance(instance.__class__.__dict__["_on_error"], types.FunctionType)
        )

    def test_instance_creation_with_non_existing_spec_with_multiple_types_expect_mock(
        self,
    ):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>",
              "<class 'pypz.core.specs.plugin.Plugin'>",
              "<class 'pypz.core.specs.plugin.PortPlugin'>",
              "<class 'pypz.core.specs.plugin.InputPortPlugin'>",
              "<class 'pypz.core.specs.plugin.ResourceHandlerPlugin'>"
            ]
          }
        }
        """

        instance = Instance.create_from_string(json_string, mock_nonexistent=True)

        self.assertTrue(issubclass(instance.__class__, InputPortPlugin))
        self.assertTrue(issubclass(instance.__class__, ResourceHandlerPlugin))

    def test_instance_creation_with_non_existing_spec_with_existing_and_non_existing_nested_instances_expect_mock(
        self,
    ):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.instance.Instance'>",
            "nestedInstances": [
              {
                "name": "existing",
                "parameters": {},
                "dependsOn": [],
                "spec": {
                  "name": "core.test.specs_tests.instance_test_resources:TestClassL3",
                  "location": null,
                  "expectedParameters": {
                    "req_str": {
                      "type": "str",
                      "required": true,
                      "description": null,
                      "currentValue": null
                    },
                    "opt_str": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": "str"
                    },
                    "optional_int": {
                      "type": "int",
                      "required": false,
                      "description": null,
                      "currentValue": 1234
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.instance.Instance'>"
                  ],
                  "nestedInstanceType": null,
                  "nestedInstances": []
                }
              },
              {
                "name": "nonexisting",
                "parameters": {},
                "dependsOn": [],
                "spec": {
                  "name": "core.test.specs_tests.instance_test_resources:NonExisting",
                  "location": null,
                  "expectedParameters": {
                    "req_str": {
                      "type": "str",
                      "required": true,
                      "description": null,
                      "currentValue": null
                    },
                    "opt_str": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": "str"
                    },
                    "optional_int": {
                      "type": "int",
                      "required": false,
                      "description": null,
                      "currentValue": 1234
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.instance.Instance'>"
                  ],
                  "nestedInstanceType": null,
                  "nestedInstances": []
                }
              }
            ]
          }
        }
        """

        instance = Instance.create_from_string(json_string, mock_nonexistent=True)

        self.assertTrue(hasattr(instance, "existing"))
        existing = instance.existing
        self.assertTrue(isinstance(existing, TestClassL3))
        self.assertNotIn("mocked", existing.__class__.__dict__)

        self.assertTrue(hasattr(instance, "nonexisting"))
        non_existing = instance.nonexisting
        self.assertTrue(isinstance(non_existing, Instance))
        self.assertIn("mocked", non_existing.__class__.__dict__)
        self.assertTrue(non_existing.__class__.__dict__["mocked"])

    def test_instance_creation_with_not_expected_nested_instance(self):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>"
            ],
            "nestedInstances": [
              {
                "name": "existing",
                "parameters": {},
                "dependsOn": [],
                "spec": {
                  "name": "core.test.specs_tests.instance_test_resources:TestClassL3",
                  "location": null,
                  "expectedParameters": {
                    "req_str": {
                      "type": "str",
                      "required": true,
                      "description": null,
                      "currentValue": null
                    },
                    "opt_str": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": "str"
                    },
                    "optional_int": {
                      "type": "int",
                      "required": false,
                      "description": null,
                      "currentValue": 1234
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.instance.Instance'>"
                  ],
                  "nestedInstanceType": null,
                  "nestedInstances": []
                }
              }
            ]
          }
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

    def test_instance_creation_with_mismatching_expected_nested_instance(self):
        os.environ["PYPZ_TEST_ENV_VAR"] = "testValue"

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": [
              "<class 'pypz.core.specs.instance.Instance'>"
            ],
            "nestedInstanceType": "<class 'pypz.core.specs.plugin.Plugin'>",
            "nestedInstances": [
              {
                "name": "existing",
                "parameters": {},
                "dependsOn": [],
                "spec": {
                  "name": "core.test.specs_tests.instance_test_resources:TestClassL3",
                  "location": null,
                  "expectedParameters": {
                    "req_str": {
                      "type": "str",
                      "required": true,
                      "description": null,
                      "currentValue": null
                    },
                    "opt_str": {
                      "type": "str",
                      "required": false,
                      "description": null,
                      "currentValue": "str"
                    },
                    "optional_int": {
                      "type": "int",
                      "required": false,
                      "description": null,
                      "currentValue": 1234
                    }
                  },
                  "types": [
                    "<class 'pypz.core.specs.instance.Instance'>"
                  ],
                  "nestedInstanceType": null,
                  "nestedInstances": []
                }
              }
            ]
          }
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

    def test_instance_creation_with_non_existing_spec_with_invalid_type_expect_error(
        self,
    ):
        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": []
          }
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass"
          }
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": ["builtins.str"]
          }
        }
        """

        with self.assertRaises(TypeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

    def test_instance_creation_with_non_existing_spec_with_non_existing_type_expect_error(
        self,
    ):
        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": ["dummy.NonExisting"]
          }
        }
        """

        with self.assertRaises(ModuleNotFoundError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

        json_string = """
        {
          "name": "instance",
          "parameters": {
            "#env": "${env:PYPZ_TEST_ENV_VAR}"
          },
          "dependsOn": [],
          "spec": {
            "name": "dummy.module:NotExistingClass",
            "types": ["pypz.NonExisting"]
          }
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

    def test_instance_creation_with_missing_required_information_spec(self):
        json_string = """
        {
        }
        """

        with self.assertRaises(ValueError):
            Instance.create_from_string(json_string)

        json_string = """
        {
          "name": "instance"
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)

        json_string = """
        {
          "name": "instance",
          "spec": {
          }
        }
        """

        with self.assertRaises(AttributeError):
            Instance.create_from_string(json_string, mock_nonexistent=True)
