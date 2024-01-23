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
from enum import Enum


class ExecutionMode(Enum):

    Skip = "Skip"
    """
    If this mode is set, then the executor will not be invoked. This is necessary
    e.g., in deployment cases, where we want to ignore the execution of certain operators.
    """

    Standard = "Standard"
    """
    In this mode the state machine will follow its standard route.
    Note that `Standard` means that resources will be created and deleted up automatically. If
    you want to have a standard mode, where resources are only created, but not deleted, then use the
    WithoutResourceDeletion
    """

    WithoutResourceDeletion = "WithoutResourceDeletion"
    """
    In this mode the state machine will follow its standard route.
    However the StateResourceDeletion will be skipped. This is a relevant use-case,
    if you want to trigger centralized resource cleanup after the operations are done.
    """

    ResourceCreationOnly = "ResourceCreationOnly"
    """
    In this mode the state machine will execute resource creation only. Note that services
    will be started before and stopped after resource creation.
    """

    ResourceDeletionOnly = "ResourceDeletionOnly"
    """
    In this mode the state machine will execute resource deletion only. Note that services
    will be started before and stopped after resource creation.
    """


class ExitCodes(Enum):

    # These are the common error codes
    # ================================

    NoError = 0
    GeneralError = 1
    CommandCannotBeExecutedError = 126
    CommandNotFoundError = 127
    InvalidExitArgumentError = 128
    FatalError = 129
    SigTerm = 130

    # These are the pypz specific error codes
    # =======================================

    StateServiceStartError = 110
    StateServiceShutdownError = 111
    StateResourceCreationError = 112
    StateResourcesDeletionError = 113
    StateOperationInitError = 114
    StateOperationError = 115
    StateOperationShutdownError = 116
