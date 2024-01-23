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
import re
import threading
import time
from typing import Generic, TypeVar, Any


def ensure_type(value, expected_type: type):
    if not isinstance(value, expected_type):
        raise TypeError(f"Expected type {expected_type} got {type(value)} instead")

    return value


def current_time_millis() -> int:
    return int(time.time() * 1000)


ReferenceType = TypeVar('ReferenceType')


class SynchronizedReference(Generic[ReferenceType]):

    def __init__(self, reference: ReferenceType):
        self.__reference: ReferenceType = reference
        self.__lock = threading.Lock()

    def get(self) -> ReferenceType:
        with self.__lock:
            return self.__reference

    def set(self, reference: ReferenceType) -> None:
        with self.__lock:
            self.__reference = reference


class TemplateResolver:

    def __init__(self, left_template_boundary: str, right_template_boundary: str):

        self.m_templatePattern = re.escape(left_template_boundary) + r'.*?' + re.escape(right_template_boundary)
        """
        Regex to find the template pattern
        """

        self.m_envVarPattern = r'(?<=' + re.escape(left_template_boundary) + r'env:)' \
                               r'(.*?)(?=' + re.escape(right_template_boundary) + r')'
        """
        Regex to find the env var specifier pattern in the template pattern
        """

    def resolve(self, lookup_object):
        """
        This method attempts to recursively resolve template strings in either a Map or Collection
        or a normal String.

        :param lookup_object: input object, where the resolution shall take place
        :return: the modified object
        """

        if isinstance(lookup_object, dict):
            new_map = dict()

            for key in lookup_object.keys():
                new_map[key] = self.resolve(lookup_object[key])

            return new_map
        elif isinstance(lookup_object, list):
            new_list = list()

            for element in lookup_object:
                new_list.append(self.resolve(element))

            return new_list
        elif isinstance(lookup_object, set):
            new_set = set()

            for element in lookup_object:
                new_set.add(self.resolve(element))

            return new_set
        elif isinstance(lookup_object, str):
            lookup_string = str(lookup_object)
            resolved_string = lookup_object

            template_matches = re.findall(self.m_templatePattern, lookup_string)

            for templateMatch in template_matches:
                resolved = None

                env_var_matches = re.findall(self.m_envVarPattern, templateMatch)

                if 0 < len(env_var_matches):
                    resolved = os.getenv(env_var_matches[0])

                resolved_string = resolved_string.replace(templateMatch, "" if resolved is None else resolved)

            return resolved_string
        else:
            return lookup_object


class InterruptableTimer:
    """
    This is a utility timer that can be interrupted in thread safe manner.
    Note that this should not be used in cases, where accuracy matters.
    """

    def __init__(self):
        self.__interrupted: SynchronizedReference[bool] = SynchronizedReference(False)

    def interrupt(self):
        self.__interrupted.set(True)

    def sleep(self, seconds: float):
        millis = round(seconds * 1000.0)
        start_time = current_time_millis()

        while ((not self.__interrupted.get()) and
               (millis >= (current_time_millis() - start_time))):
            pass


def convert_to_dict(obj: Any) -> Any:
    """
    This method attempts to convert an object recursively to a dictionary
    """

    if isinstance(obj, list):
        return [convert_to_dict(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_to_dict(item) for item in obj)
    elif isinstance(obj, set):
        return {convert_to_dict(item) for item in obj}
    elif isinstance(obj, dict):
        return {key: convert_to_dict(value) for key, value in obj.items()}
    elif hasattr(obj, '__dict__'):
        return {key: convert_to_dict(value) for key, value in obj.__dict__.items()}
    else:
        return obj


def is_type_allowed(obj, allowed_types: tuple) -> bool:
    """
    This function checks if the type of the provided object is allowed given
    the tuple of allowed types provided as argument.
    """

    if not isinstance(obj, allowed_types):
        return False

    if isinstance(obj, list):
        return all(is_type_allowed(item, allowed_types) for item in obj)
    elif isinstance(obj, tuple):
        return all(is_type_allowed(item, allowed_types) for item in obj)
    elif isinstance(obj, set):
        return all(is_type_allowed(item, allowed_types) for item in obj)
    elif isinstance(obj, dict):
        return all(is_type_allowed(value, allowed_types) for value in obj.values())
    elif hasattr(obj, '__dict__'):
        return all(is_type_allowed(value, allowed_types) for value in obj.__dict__.values())

    return True
