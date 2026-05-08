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
from typing import Optional

from pypz.core.commons.parameters import OptionalParameter, RequiredParameter
from pypz.core.specs.instance import Instance, InstanceGroup, ReplicaContext
from pypz.core.specs.misc import BlankInstance


class TestClassL0(BlankInstance[Instance]):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.l10 = TestClassL1()
        self.l11 = TestClassL1("other_name")

        self.l10.depends_on(self.l11)

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234


class TestClassL1(BlankInstance[Instance]):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.l2 = TestClassL2()

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234


class TestClassL2(BlankInstance[Instance]):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.l3 = TestClassL3()

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234


class TestClassL3(BlankInstance[None]):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)

        self.l4 = TestClassL4()

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234


class TestClassL4(BlankInstance[None]):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234


class TestCommonParentClass(BlankInstance[None]):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)


class TestExtendedClassA(TestCommonParentClass):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)


class TestExtendedClassB(TestCommonParentClass):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, *args, **kwargs)


class TestClassWithDifferentNestedType(BlankInstance[TestCommonParentClass]):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, TestCommonParentClass, *args, **kwargs)

        self.a = TestExtendedClassA()
        self.b = TestExtendedClassB()
        self.c = TestExtendedClassB()

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234


class TestClassForDependencyResolution(BlankInstance[Instance]):

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.instance_0 = BlankInstance()
        self.instance_1 = BlankInstance()
        self.instance_2 = BlankInstance()
        self.instance_3 = BlankInstance()
        self.instance_4 = BlankInstance()


class CustomParameterClass:
    def __init__(self):
        self._dict = {"a": 0, "b": 0}
        self._list = [0, 1, 2]
        self._set = {0, 1, 2}


class TestReplicableClassL0(BlankInstance[Instance], InstanceGroup):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.l1 = TestReplicableClassL1()

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234

    def get_l1_full_name(self):
        return self.l1.get_full_name()

    def get_l2_full_name(self):
        return self.l1.l2.get_full_name()

    def get_group_size(self) -> int:
        return 0

    def get_group_index(self) -> int:
        return 0

    def get_group_name(self) -> Optional[str]:
        return self.get_full_name()

    def get_group_principal(self) -> Optional["Instance"]:
        return self

    def is_principal(self) -> bool:
        return True


class TestReplicableClassWithNestedReplicaL0(BlankInstance[Instance], InstanceGroup):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.l1 = TestReplicableClassL1()
        self.l12 = TestReplicableClassL1()
        self.l1_r0 = ReplicaContext(self.l1, 0)

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234

    def get_group_size(self) -> int:
        return 0

    def get_group_index(self) -> int:
        return 0

    def get_group_name(self) -> Optional[str]:
        return self.get_full_name()

    def get_group_principal(self) -> Optional["Instance"]:
        return self

    def is_principal(self) -> bool:
        return True


class TestReplicableClassL1(BlankInstance[Instance], InstanceGroup):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.l2 = TestReplicableClassL2()

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234

    def get_l2_full_name(self):
        return self.l2.get_full_name()

    def get_group_size(self) -> int:
        return self.get_context().get_parameter("replicationFactor")

    def get_group_index(self) -> int:
        return 0

    def get_group_name(self) -> Optional[str]:
        return self.get_full_name()

    def get_group_principal(self) -> Optional["Instance"]:
        return self

    def is_principal(self) -> bool:
        return True


class TestReplicableClassL2(BlankInstance[Instance], InstanceGroup):

    req_str = RequiredParameter(str)
    opt_str = OptionalParameter(str)
    opt_int = OptionalParameter(int, alt_name="optional_int")

    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, Instance, *args, **kwargs)

        self.req_str = None
        self.opt_str = "str"
        self.opt_int = 1234

    def get_group_size(self) -> int:
        return self.get_context().get_parameter("replicationFactor")

    def get_group_index(self) -> int:
        return 0

    def get_group_name(self) -> Optional[str]:
        return self.get_full_name()

    def get_group_principal(self) -> Optional["Instance"]:
        return self

    def is_principal(self) -> bool:
        return True


class TestReplicableClassWithNameAccessInCtor(BlankInstance[None], InstanceGroup):
    def __init__(self, name: str = None, *args, **kwargs):
        super().__init__(name, None, *args, **kwargs)
        self.full_name = self.get_full_name()

    def get_group_size(self) -> int:
        pass

    def get_group_index(self) -> int:
        pass

    def get_group_name(self) -> Optional[str]:
        pass

    def get_group_principal(self) -> Optional["Instance"]:
        pass

    def is_principal(self) -> bool:
        pass
