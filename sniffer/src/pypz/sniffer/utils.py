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
from pypz.core.specs.operator import Operator
from pypz.core.specs.pipeline import Pipeline
from pypz.core.specs.plugin import OutputPortPlugin


def is_sublist(subset: list, target_list: list) -> bool:
    if (not subset) or (0 == len(subset)):
        return True

    subset_len = len(subset)
    target_len = len(target_list)

    if subset_len > target_len:
        return False

    for i in range(target_len - subset_len + 1):
        if target_list[i : i + subset_len] == subset:
            return True

    return False


def retrieve_operator_paths(
    operator: Operator, visited: set[Operator] = None
) -> list[list[Operator]]:
    result_paths = []
    visited = {operator} if visited is None else visited
    for plugin in operator.get_protected().get_nested_instances().values():
        if isinstance(plugin, OutputPortPlugin):
            for input_port in plugin.get_connected_ports():
                if input_port.get_context() not in visited:
                    result_paths.extend(
                        [
                            [operator] + retrieved_path
                            for retrieved_path in retrieve_operator_paths(
                                input_port.get_context(), visited
                            )
                        ]
                    )
    return result_paths if 0 < len(result_paths) else [[operator]]


def order_operators_by_connections(pipeline: Pipeline) -> list[set[Operator]]:
    """
    This function attempts to order the operators along their connections. Consider
    the following example with connection directions from top to bottom:

    ::
       | A   B |
       |  \ /  |
       |   C   |
       |  / \  |
       | D   E |
       | |  /  |
       | F /   |
       | |/    |
       | G     |

    Dependencies can be expressed along the connections. The expected result from this
    function orders these dependencies into list of sets, where the list represents
    the dependency levels and the set the independent operators on each level.
    Expected result for the example:

    ::

       [0] - {A,B}
       [1] - {C}
       [2] - {D,E}
       [3] - {F}
       [4] - {G}

    This information can then be used amongst more to draw the operators in the proper
    order and position to visualize their connections.
    Note that this method is capable to handle circular dependencies.
    """

    paths: list = []
    # Step 1)
    # Extracting the paths. Using the example in the docs, the expected result:
    # [0] - [A, C, D, F, G]
    # [1] - [A, C, E, G]
    # [2] - [B, C, E, G]
    # [3] - [B, C, D, F, G]
    # =========================================================================
    for operator in pipeline.get_protected().get_nested_instances().values():
        if operator.is_principal() and (not any([operator in path for path in paths])):
            paths.extend(retrieve_operator_paths(operator))

    # Step 2)
    # Removing all paths that are sublist of any other paths. This is necessary,
    # because we don't have control over, in which order the operators are stored
    # and iterated over, so there can be a situation, where the previous step
    # produces something like this:
    # [0] - [E, G]
    # [1] - [C, E, G]
    # [2] - [A, C, E, G]
    # The first 2 is sub-path of the 3. therefore shall be removed.
    # ============================================================================
    cleaned_paths = [
        path
        for path in paths
        if not any(
            [
                is_sublist(path, target_path) and (path != target_path)
                for target_path in paths
            ]
        )
    ]

    # Step 3)
    # Converting path into list of sets. Using the example in the docs, the expected result:
    # [0] -> {A, B}
    # [1] -> {C}
    # [2] -> {D, E}
    # [3] -> {F, G}
    # [4] -> {G}
    # =======================================================================================
    dependency_levels: list = []
    for path in cleaned_paths:
        for idx, node in enumerate(path):
            if idx == len(dependency_levels):
                dependency_levels.append(set())
            dependency_levels[idx].add(node)

    # Step 4)
    # We need to make sure that each operator is represented only once in the ordered list. The rule
    # is that we start to iterate from the end of the list and the first one of the duplicates found
    # survives, the rest shall be removed.
    # Using the example results from step 3), the expected result is:
    # [0] -> {A, B}
    # [1] -> {C}
    # [2] -> {D, E}
    # [3] -> {F}
    # [4] -> {G}
    # Notice that G has been removed from [3], because it has been found on [4]
    # ================================================================================================

    found = set()
    for dependency_level in dependency_levels:
        set_copy = dependency_level.copy()
        for operator in set_copy:
            if operator not in found:
                found.add(operator)
            else:
                dependency_level.remove(operator)

    return dependency_levels
