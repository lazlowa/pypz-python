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
import signal
import socket
import sys
import traceback

from pypz.executors.commons import ExecutionMode
from pypz.executors.commons import ExitCodes
from pypz.core.commons.utils import SynchronizedReference, TemplateResolver
from pypz.core.specs.operator import Operator
from pypz.core.specs.plugin import ExtendedPlugin, Plugin
from pypz.executors.operator.context import ExecutionContext
from pypz.executors.operator.signals import BaseSignal, SignalNoOp, SignalKill, SignalServicesStart, SignalShutdown, \
    SignalResourcesCreation, SignalResourcesDeletion, SignalError, SignalOperationInit, SignalServicesStop, \
    SignalOperationStart, SignalOperationStop
from pypz.executors.operator.states import State, StateEntry, StateKilled, StateOperationInit, StateOperationShutdown, \
    StateOperationRunning, StateResourceCreation, StateResourceDeletion, StateServiceStart, StateServiceShutdown
from pypz.version import PROJECT_VERSION


class OperatorExecutor:
    """
    This class has the purpose of executing an Operator along with its nested
    plugins. The execution is based on a state machine, where each state
    is responsible to execute specific entities' corresponding methods.

    :param operator: the actual operator instance to execute
    :param handle_interrupts: if True, then the execution can be interrupted by system signals
    """

    def __init__(self, operator: Operator, handle_interrupts: bool = True):

        # Initializing shutdown hook and signal handling as early as possible to prevent
        # the case that the signal has been sent during startup, but missed by the executors
        if handle_interrupts:
            signal.signal(signal.SIGTERM, self.interrupt)
            signal.signal(signal.SIGINT, self.interrupt)

        self.__operator: Operator = operator
        """
        The actual operator instance, which shall be processed by the state machine
        """

        self.__context: ExecutionContext | None = None
        """
        The stored execution context for this executor
        """

        self.__priority_signal: SynchronizedReference[BaseSignal] = SynchronizedReference(SignalNoOp())
        """
        Reference to the priority signal.
        Note that this atomic variable was necessary, because there might be functionality that attempts
        to change the current signal. Direct changing is not really a lucky choice if changing from a separate
        thread is allowed (e.g. from shutdown hook). Therefore all the external entities are allowed to change
        the priority signal
        """

        self.__is_running: SynchronizedReference[bool] = SynchronizedReference(False)
        """
        Flag that signalizes, whether the state machine is running
        """

        # ======= State declarations ========

        self.__state_entry: State | None = None
        self.__state_killed: State | None = None
        self.__state_operation_init: State | None = None
        self.__state_operation_running: State | None = None
        self.__state_operation_shutdown: State | None = None
        self.__state_resource_creation: State | None = None
        self.__state_resource_deletion: State | None = None
        self.__state_service_start: State | None = None
        self.__state_service_shutdown: State | None = None

        self.__current_state: State | None = None
        self.__current_signal: BaseSignal | None = None

        # Parameter handling
        # ==================

        # Resolve runtime templates as well for plugins
        for name, value in self.__operator.get_protected().get_parameters().items():
            self.__operator.set_parameter(name, TemplateResolver("$(", ")").resolve(value))

        for plugin in self.__operator.get_protected().get_nested_instances().values():
            for name, value in plugin.get_protected().get_parameters().items():
                plugin.set_parameter(name, TemplateResolver("$(", ")").resolve(value))

        # This is the point, where required parameters shall be checked, before continue
        missing_required_parameters = self.__operator.get_missing_required_parameters()

        if 0 < len(missing_required_parameters):
            raise LookupError(f"[{self.__operator.get_full_name()}] "
                              f"Missing required parameters: {missing_required_parameters}")

        # Mock instance checking
        # ======================
        # Mocked operators cannot be executed
        if "mocked" in operator.__class__.__dict__:
            raise PermissionError(f"[{operator.get_full_name()}] Mock operator cannot be executed")

        # If a non-mock operator has a mock plugin, then something went wrong
        for plugin in self.__operator.get_protected().get_nested_instances().values():
            if "mocked" in plugin.__class__.__dict__:
                raise PermissionError(f"[{plugin.get_full_name()}] Mock plugin in regular operator")

    # ========= public methods ==========

    def is_running(self):
        return self.__is_running.get()

    def get_current_state(self):
        return self.__current_state

    def get_current_signal(self):
        return self.__current_signal

    def execute(self, exec_mode: ExecutionMode = ExecutionMode.Standard) -> int:
        """
        Initializes and triggers execution. Note that, since every time, if this method is
        called, the persisted state of the execution and of its context is re-initialized,
        hence this method is idempotent.

        :param exec_mode: the run mode if the execution (refer to ExecutorRunMode)
        :return: exit code (refer to ExitCodes)
        """

        if ExecutionMode.Skip == exec_mode:
            return 0

        try:
            # ========= Initialize instances =========

            self.__initialize(exec_mode)

            # ========= Start the state machine =========

            try:
                self.__operator.get_logger().debug(f"Host: {os.getenv('PYPZ_NODE_NAME', socket.gethostname())}")

                self.__operator.get_logger().debug(f"Version: {PROJECT_VERSION}")

                self.__operator.get_logger().debug("Starting state machine ...")

                self.__operator.get_logger().debug("Run mode: %s", exec_mode.name)

                self.__is_running.set(True)

                while not isinstance(self.__current_signal, SignalKill):
                    self.__current_signal = self.__current_state.on_execute()

                    if not isinstance(self.__priority_signal.get(), SignalNoOp):
                        self.__operator.get_logger().debug("Priority signal caught: %s",
                                                           self.__priority_signal.get().__class__.__name__)
                        self.__current_signal = self.__priority_signal.get()
                        self.__priority_signal.set(SignalNoOp())

                    self.__current_state = self.__current_state.on_signal_handling(self.__current_signal)

            except:  # noqa: E722
                self.__context.set_exit_code(ExitCodes.FatalError)
                self.__operator.get_logger().error(traceback.format_exc())
            finally:
                self.__is_running.set(False)

                self.__operator.get_logger().debug("Shutting down state machine ...")
                self.__state_entry.shutdown()
                self.__state_killed.shutdown()
                self.__state_operation_init.shutdown()
                self.__state_operation_shutdown.shutdown()
                self.__state_operation_running.shutdown()
                self.__state_resource_creation.shutdown()
                self.__state_resource_deletion.shutdown()
                self.__state_service_start.shutdown()
                self.__state_service_shutdown.shutdown()

                try:
                    self.__context.for_each_plugin_objects_with_type(
                        ExtendedPlugin, lambda plugin: plugin.get_protected().post_execution())
                except Exception as e:  # noqa: F841
                    self.__context.for_each_plugin_objects_with_type(
                        ExtendedPlugin, lambda plugin: plugin.get_protected().on_error(self.__class__, e)  # noqa: F821
                    )
                    raise
        except:  # noqa: E722
            # Catching exceptions not handled at this point
            traceback.print_exc(file=sys.stderr)
            self.__context.set_exit_code(ExitCodes.FatalError)

        if ExitCodes.NoError != self.__context.get_exit_code():
            print("[ERROR] Error occurred during the execution. Use logger plugin for details.", file=sys.stderr)

        return self.__context.get_exit_code().value

    # ========= private methods ==========

    def __initialize(self, exec_mode: ExecutionMode = ExecutionMode.Standard) -> None:
        """
        Initializes the execution states and context.

        :param exec_mode: the run mode if the execution (refer to ExecutorRunMode)
        """

        # ========= Initialize context =========

        self.__context: ExecutionContext = ExecutionContext(self.__operator, exec_mode)

        try:
            # Addons shall be initialized as early as possible to cover the most part
            # of the execution
            self.__context.for_each_plugin_objects_with_type(
                ExtendedPlugin, lambda plugin: plugin.get_protected().pre_execution())
        except Exception as e:  # noqa: F841
            self.__context.for_each_plugin_objects_with_type(
                ExtendedPlugin, lambda plugin: plugin.get_protected().on_error(self.__class__, e)  # noqa: F821
            )
            raise

        # ========= Initialize state machine =========

        # State transitions init
        # ======================

        self.__state_entry = StateEntry(self.__context)
        self.__state_killed = StateKilled(self.__context)
        self.__state_operation_init = StateOperationInit(self.__context)
        self.__state_operation_shutdown = StateOperationShutdown(self.__context)
        self.__state_operation_running = StateOperationRunning(self.__context)
        self.__state_resource_creation = StateResourceCreation(self.__context)
        self.__state_resource_deletion = StateResourceDeletion(self.__context)
        self.__state_service_start = StateServiceStart(self.__context)
        self.__state_service_shutdown = StateServiceShutdown(self.__context)

        self.__state_entry.set_transition(SignalServicesStart, self.__state_service_start)
        self.__state_entry.set_transition(SignalShutdown, self.__state_killed)

        self.__state_service_start.set_transition(SignalResourcesCreation, self.__state_resource_creation)
        self.__state_service_start.set_transition(SignalResourcesDeletion, self.__state_resource_deletion)
        self.__state_service_start.set_transition(SignalError, self.__state_service_shutdown)
        self.__state_service_start.set_transition(SignalShutdown, self.__state_service_shutdown)

        self.__state_resource_creation.set_transition(SignalOperationInit, self.__state_operation_init)
        self.__state_resource_creation.set_transition(SignalError, self.__state_resource_deletion)
        self.__state_resource_creation.set_transition(SignalServicesStop, self.__state_service_shutdown)
        self.__state_resource_creation.set_transition(SignalShutdown, self.__state_resource_deletion)

        self.__state_operation_init.set_transition(SignalOperationStart, self.__state_operation_running)
        self.__state_operation_init.set_transition(SignalError, self.__state_operation_shutdown)
        self.__state_operation_init.set_transition(SignalShutdown, self.__state_operation_shutdown)

        self.__state_operation_running.set_transition(SignalOperationStop, self.__state_operation_shutdown)
        self.__state_operation_running.set_transition(SignalError, self.__state_operation_shutdown)
        self.__state_operation_running.set_transition(SignalShutdown, self.__state_operation_shutdown)

        self.__state_operation_shutdown.set_transition(SignalResourcesDeletion, self.__state_resource_deletion)
        self.__state_operation_shutdown.set_transition(SignalServicesStop, self.__state_service_shutdown)
        self.__state_operation_shutdown.set_transition(SignalError, self.__state_resource_deletion)

        self.__state_resource_deletion.set_transition(SignalServicesStop, self.__state_service_shutdown)
        self.__state_resource_deletion.set_transition(SignalError, self.__state_service_shutdown)

        self.__state_service_shutdown.set_transition(SignalError, self.__state_killed)
        self.__state_service_shutdown.set_transition(SignalKill, self.__state_killed)

        self.__current_state = self.__state_entry
        self.__current_signal = SignalNoOp()

    def interrupt(self, signal_number=None, current_stack=None):
        if self.__is_running.get():
            self.__operator.get_logger().debug("Processing interrupt signal ...")

            # Current state shutdown shall be called here to interrupt scheduling and cancel futures
            self.__current_state.shutdown()

            # Invoking plugins' on_interrupt() method
            try:
                self.__context.for_each_plugin_objects_with_type(
                    Plugin, lambda plugin: plugin.get_protected().on_interrupt(signal_number))
            except:  # noqa: E722
                # Ignore exception to be able to proceed with the shutdown
                traceback.print_exc(file=sys.stderr)
                pass

            # Invoking operator's on_interrupt() method
            try:
                self.__operator.get_protected().on_interrupt(signal_number)
            except:  # noqa: E722
                # Ignore exception to be able to proceed with the shutdown
                traceback.print_exc(file=sys.stderr)
                pass

            self.__context.set_exit_code(ExitCodes.SigTerm)
            self.__priority_signal.set(SignalShutdown())
