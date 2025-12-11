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
from __future__ import annotations

import concurrent.futures
import inspect
import time
import traceback
import types
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional, Type

from pypz.core.commons.loggers import ContextLogger
from pypz.core.commons.utils import current_time_millis
from pypz.core.specs.instance import Instance
from pypz.core.specs.plugin import (
    InputPortPlugin,
    PortPlugin,
    ResourceHandlerPlugin,
    ServicePlugin,
)
from pypz.executors.commons import ExecutionMode, ExitCodes
from pypz.executors.operator.context import ExecutionContext
from pypz.executors.operator.signals import (
    BaseSignal,
    SignalError,
    SignalKill,
    SignalNoOp,
    SignalOperationInit,
    SignalOperationStart,
    SignalOperationStop,
    SignalResourcesCreation,
    SignalResourcesDeletion,
    SignalServicesStart,
    SignalServicesStop,
    SignalTerminate,
)


class State(ABC):
    """
    This class is used to collect and abstract the common logic in all derived states.
    All other states must extend on this class.

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    # ============ inner classes =============

    class Execution:
        def __init__(self, method: Any, instances: set[Instance], *args, **kwargs):
            self.method = method
            self.instances = instances
            self.args = args
            self.kwargs = kwargs

    class MethodWrapper:
        """
        This class represents the invocation of the plugin objects' method as action. It
        is used by both the parallel and the sequential executor.

        :param state: the state object, where the execution is performed
        :param instance_name: name of the executable instance
        :param callable_method: the method of the instance to be invoked
        """

        def __init__(
            self, state: State, instance_name: str, callable_method: Callable[..., bool]
        ):
            self.__state = state
            self.__instance_name = instance_name
            self.__callable_method = callable_method

        def __call__(self, *args, **kwargs):

            before_timestamp_ms = current_time_millis()

            try:
                # Invocation of the actual plugin object's method, which was given as
                # parameter to this class
                is_completed = self.__callable_method(*args, **kwargs)

                # If method's return type has been annotated, then we need to check, if the
                # method indeed returns the expected type. Raising error if not.
                method_annotations = inspect.get_annotations(self.__callable_method)

                # Retrieve the return type. Notice that it might be None in which case we need to set
                # the type to NoneType to allow checking isinstance
                return_type = (
                    None
                    if "return" not in method_annotations
                    else (
                        types.NoneType
                        if method_annotations.get("return") is None
                        else method_annotations.get("return")
                    )
                )

                if (
                    (return_type is not None)
                    and (return_type is not Optional)
                    and (not isinstance(is_completed, return_type))
                ):
                    raise TypeError(
                        f"Invalid return type: {type(is_completed)}, boolean expected."
                    )
            finally:
                after_timestamp_ms = current_time_millis()
                elapsed_time_ms = after_timestamp_ms - before_timestamp_ms

                self.__state._logger.debug(
                    "%s | Method: %s; State: %s; Start timestamp: %s; "
                    "Elapsed time: %s [ms]",
                    self.__instance_name,
                    self.__callable_method.__name__,
                    self.__state.__class__.__name__,
                    before_timestamp_ms,
                    elapsed_time_ms,
                )

            return is_completed

    # ============ ctor =============

    def __init__(self, context: ExecutionContext, *args, **kwargs):

        self.__executor: ThreadPoolExecutor = (
            ThreadPoolExecutor(thread_name_prefix=self.__class__.__name__)
            if "executor_pool_size" not in kwargs
            else ThreadPoolExecutor(
                kwargs["executor_pool_size"], thread_name_prefix=self.__class__.__name__
            )
        )
        """
        Handles the execution of plugins' corresponding methods, which can run parallel
        """

        self._transition_map: dict[Type[BaseSignal], State] = {}
        """
        Contains the possible transitions from this state
        """

        self._reason: Optional[BaseSignal] = None
        """
        Contains the information about the signal that drove the
        state machine into this state i.e., the reason of this state
        """

        self._prev_state: Optional[State] = None
        """
        Contains the information about the previous state the
        state machine was in
        """

        self._context: ExecutionContext = context
        """
        Holds the reference to the actual operator instance that shall be executed
        """

        self._state_entry_time_ms: int = 0
        """
        Stores the state entry epoch time
        """

        self._response_collector: dict[Callable[[], bool], bool] = {}
        """
        Stores the responses of the plugin objects for each instance. Used to prevent finished
        instances to be re-scheduled
        """

        self._logger: ContextLogger = ContextLogger(
            self._context.get_operator().get_logger(),
            self._context.get_operator().get_full_name(),
            self.__class__.__name__,
        )

    # ======== abstract methods =========

    @abstractmethod
    def on_entry(self) -> None:
        pass

    @abstractmethod
    def on_exit(self) -> None:
        pass

    @abstractmethod
    def on_execute(self) -> BaseSignal:
        pass

    # ======== public methods =========

    def get_prev_state(self) -> Optional[State]:
        return self._prev_state

    def get_transitions(self):
        return self._transition_map

    def set_transition(self, signal: Type[BaseSignal], new_state: State) -> None:
        self._logger.debug(
            "Setting up transition: (%s)--[%s]-->(%s)",
            self.__class__.__name__,
            signal.__name__,
            new_state.__class__.__name__,
        )

        if signal in self._transition_map:
            raise AttributeError(
                f"Signal already registered in transition map: {signal}"
            )

        self._transition_map[signal] = new_state

    def on_signal_handling(self, signal: BaseSignal) -> State:
        if not isinstance(signal, BaseSignal):
            raise AttributeError(f"Invalid signal type: {type(signal)}")

        if signal.__class__ in self._transition_map:
            new_state = self._transition_map[signal.__class__]

            new_state._reason = signal
            new_state._prev_state = self

            self.__exit_state()
            new_state.__enter_state()

            return new_state
        else:
            if not isinstance(signal, SignalNoOp):
                self._logger.warning(
                    "Unhandled signal in '%s': %s",
                    self.__class__.__name__,
                    signal.__class__.__name__,
                )

            return self

    def _schedule(
        self, *execution_chain: Execution, break_on_exception: bool = False
    ) -> bool:
        """
        Schedules a chain of instances' specified methods for execution via the thread pool
        executor. Each chain element is represented by a list of instances and the method to
        be called. Each list will be executed parallel. The next chain element will be
        executed after all instance methods have been concluded on the current chain element.
        This method blocks until all the scheduled methods are finished.
        Both plugin and operator instance methods can be scheduled.

        :param execution_chain: list of tuples, where [0] is the callable method and [1] is the list of instances
        :param break_on_exception: if True, it will break on the first exception, if False, it will let then chain run
                                   and only raise an exception at the end
        :return: True if all methods are finished (returned True), False otherwise
        """

        all_instances_finished: bool = True
        error_in_instances: list[str] = []

        for execution in execution_chain:

            method_name = (
                execution.method
                if isinstance(execution.method, str)
                else execution.method.__name__  # type: ignore
            )

            futures: dict[concurrent.futures.Future, Callable[[], Any]] = {}
            method_to_instance: dict[Callable[[], Any], Instance] = {}

            for instance in execution.instances:
                # Early termination in case an actual instance object does not implement the
                # specified method
                if not hasattr(instance, method_name):
                    continue

                method_reference = getattr(instance, method_name)

                if not isinstance(method_reference, types.MethodType):
                    raise TypeError(
                        f"Invalid callable method type: {type(method_reference)}. "
                        f"Must be FunctionType of Callable[[], bool]."
                    )

                method_to_instance[method_reference] = instance

                if method_reference not in self._response_collector:
                    self._response_collector[method_reference] = False

                if not self._response_collector[method_reference]:
                    futures[
                        self.__executor.submit(
                            State.MethodWrapper(
                                self, instance.get_simple_name(), method_reference
                            ),
                            *execution.args,
                            **execution.kwargs,
                        )
                    ] = method_reference

            for future, method_reference in futures.items():
                # Early termination, if the future has been prematurely cancelled. This can be
                # the case, if a shutdown hook has been caught and the state executors has been
                # shut down in it
                if future.cancelled():
                    continue

                try:
                    self._response_collector[method_reference] = future.result()

                    # With this logic we make sure that once the flag has been set to False
                    # it remains False i.e., if one instance is not complete the entire
                    # chain is not complete
                    if all_instances_finished and isinstance(
                        self._response_collector[method_reference], bool
                    ):
                        all_instances_finished = self._response_collector[
                            method_reference
                        ]
                except Exception as e:
                    self._logger.error(
                        f"Exception at {method_reference} of "
                        f"{method_to_instance[method_reference].get_simple_name()}: {e}"
                    )
                    self._logger.error(traceback.format_exc())
                    error_in_instances.append(
                        method_to_instance[method_reference].get_simple_name()
                    )
                    if break_on_exception:
                        raise RuntimeError(self.__class__.__name__, error_in_instances)

        if 0 < len(error_in_instances):
            raise RuntimeError(self.__class__.__name__, error_in_instances)

        return all_instances_finished

    def shutdown(self):
        self._logger.debug("Shutting down state: %s", self.__class__.__name__)
        self.__executor.shutdown(wait=True, cancel_futures=True)

    # ======== private methods =========

    def __enter_state(self):
        self._logger.info(
            "(%s)--[%s]-->(%s)",
            self._prev_state.__class__.__name__,
            self._reason.__class__.__name__,
            self.__class__.__name__,
        )

        self._response_collector.clear()

        self.on_entry()

        self._state_entry_time_ms = current_time_millis()

    def __exit_state(self):
        self.on_exit()


class StateEntry(State):
    """
    Serves as entry point for the state machine.

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        return SignalServicesStart()


class StateKilled(State):
    """
    Serves as exit point for the state machine.

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        return SignalTerminate()


class StateOperationInit(State):
    """
    This state initializes the operator.

    Invoked methods:

    1. :meth:`pypz.core.specs.plugin.PortPlugin._on_port_open`
    2. :meth:`pypz.core.specs.operator.Operator._on_init`

    This order of execution guarantees that the Operator's implementation is already having access
    to the ports in the init phase.


    .. note::
       Note that both the :class:`InputPortPlugin <pypz.core.specs.plugin.InputPortPlugin>` and
       the :class:`OutputPortPlugin <pypz.core.specs.plugin.OutputPortPlugin>` inherits from the
       :class:`PortPlugin <pypz.core.specs.plugin.PortPlugin>`, hence plugins of both types will
       be initialized.

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        try:
            if not self._schedule(
                *[
                    State.Execution("_on_port_open", level)
                    for level in self._context.get_dependency_graph_by_type(PortPlugin)
                ],
                State.Execution("_on_init", {self._context.get_operator()}),
                break_on_exception=True,
            ):
                time.sleep(1)
                return SignalNoOp()

            return SignalOperationStart()
        except Exception as e:
            self._logger.error("========== Exception at execution ==========")
            self._logger.error(traceback.format_exc())
            self._logger.error("============================================")

            try:
                self._schedule(
                    State.Execution(
                        "_on_error",
                        {self._context.get_operator()},
                        source=self.__class__,
                        exception=e.__context__,
                    ),
                    State.Execution(
                        "_on_error",
                        self._context.get_plugin_instances_by_type(PortPlugin),
                        source=self.__class__,
                        exception=e.__context__,
                    ),
                )
            except Exception as ex:
                self._logger.error(f"Exception at error handling: {ex}")
                self._logger.error(traceback.format_exc())

            self._context.set_exit_code(ExitCodes.StateOperationInitError)

            return SignalError(e)


class StateOperationRunning(State):
    """
    This state calls the main processing method of the Operator:

    :meth:`pypz.core.specs.operator.Operator._on_running`

    After successful finish of the Operator's method, the offset commit on all the
    :class:`InputPortPlugin <pypz.core.specs.plugin.InputPortPlugin>` will be invoked.

    :meth:`pypz.core.specs.plugin.InputPortPlugin.commit_current_read_offset`

    This ensures that even, if the developer did not commit offsets manually in the
    implementation, offsets will still be committed.

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        try:
            is_finished = self._context.get_operator()._on_running()
            # TODO - on_running shall be submitted to the executor as well
            # is_finished = self._schedule(State.Execution("_on_running", {self._context.get_operator()}))
            self._schedule(
                State.Execution(
                    "commit_current_read_offset",
                    self._context.get_plugin_instances_by_type(InputPortPlugin),
                )
            )

            # If nothing or None is returned from on_running, then it will be automatically
            # determined, whether to terminate the state or not
            if is_finished is None:
                for input_port_plugin in self._context.get_plugin_instances_by_type(
                    InputPortPlugin
                ):
                    if input_port_plugin.can_retrieve():
                        return SignalNoOp()
            elif not is_finished:
                return SignalNoOp()

            return SignalOperationStop()
        except Exception as e:
            self._logger.error("========== Exception at execution ==========")
            self._logger.error(traceback.format_exc())
            self._logger.error("============================================")

            try:
                self._schedule(
                    State.Execution(
                        "_on_error",
                        {self._context.get_operator()},
                        source=self.__class__,
                        exception=e.__context__,
                    ),
                    State.Execution(
                        "_on_error",
                        self._context.get_plugin_instances_by_type(PortPlugin),
                        source=self.__class__,
                        exception=e.__context__,
                    ),
                )
            except Exception as ex:
                self._logger.error(f"Exception at error handling: {ex}")
                self._logger.error(traceback.format_exc())

            self._context.set_exit_code(ExitCodes.StateOperationError)

            return SignalError(e)


class StateOperationShutdown(State):
    """
    This state shuts down the operator.

    Invoked methods:

    1. :meth:`pypz.core.specs.operator.Operator._on_shutdown`
    2. :meth:`pypz.core.specs.plugin.PortPlugin._on_port_close`

    This order of execution guarantees that the Operator's implementation is still having access
    to the ports in the shutdown phase.

    .. note::
       Note that both the :class:`InputPortPlugin <pypz.core.specs.plugin.InputPortPlugin>` and
       the :class:`OutputPortPlugin <pypz.core.specs.plugin.OutputPortPlugin>` inherits from the
       :class:`PortPlugin <pypz.core.specs.plugin.PortPlugin>`, hence plugins of both types will
       be shut down.

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        try:
            if not self._schedule(
                State.Execution("_on_shutdown", {self._context.get_operator()}),
                *[
                    State.Execution("_on_port_close", level)
                    for level in reversed(
                        self._context.get_dependency_graph_by_type(PortPlugin)
                    )
                ],
            ):
                time.sleep(1)
                return SignalNoOp()

            if (
                ExecutionMode.WithoutResourceDeletion
                == self._context.get_execution_mode()
            ):
                return SignalServicesStop()

            return SignalResourcesDeletion()
        except Exception as e:
            self._logger.error("========== Exception at execution ==========")
            self._logger.error(traceback.format_exc())
            self._logger.error("============================================")

            try:
                self._schedule(
                    State.Execution(
                        "_on_error",
                        {self._context.get_operator()},
                        source=self.__class__,
                        exception=e.__context__,
                    ),
                    State.Execution(
                        "_on_error",
                        self._context.get_plugin_instances_by_type(PortPlugin),
                        source=self.__class__,
                        exception=e.__context__,
                    ),
                )
            except Exception as ex:
                self._logger.error(f"Exception at error handling: {ex}")
                self._logger.error(traceback.format_exc())

            self._context.set_exit_code(ExitCodes.StateOperationShutdownError)

            return SignalError(e)


class StateResourceCreation(State):
    """
    This state is responsible to invoke the resource creation related methods.

    Invoked methods:

    :meth:`pypz.core.specs.plugin.ResourceHandlerPlugin._on_resource_creation`

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        try:
            if not self._schedule(
                *[
                    State.Execution("_on_resource_creation", level)
                    for level in self._context.get_dependency_graph_by_type(
                        ResourceHandlerPlugin
                    )
                ],
                break_on_exception=True,
            ):
                time.sleep(1)
                return SignalNoOp()

            if ExecutionMode.ResourceCreationOnly == self._context.get_execution_mode():
                return SignalServicesStop()

            return SignalOperationInit()
        except Exception as e:
            self._logger.error("========== Exception at execution ==========")
            self._logger.error(traceback.format_exc())
            self._logger.error("============================================")

            try:
                self._schedule(
                    State.Execution(
                        "_on_error",
                        self._context.get_plugin_instances_by_type(
                            ResourceHandlerPlugin
                        ),
                        source=self.__class__,
                        exception=e.__context__,
                    )
                )
            except Exception as ex:
                self._logger.error(f"Exception at error handling: {ex}")
                self._logger.error(traceback.format_exc())

            self._context.set_exit_code(ExitCodes.StateResourceCreationError)

            return SignalError(e)


class StateResourceDeletion(State):
    """
    This state is responsible to invoke the resource deletion related methods.

    Invoked methods:

    :meth:`pypz.core.specs.plugin.ResourceHandlerPlugin._on_resource_deletion`

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        try:
            if not self._schedule(
                *[
                    State.Execution("_on_resource_deletion", level)
                    for level in reversed(
                        self._context.get_dependency_graph_by_type(
                            ResourceHandlerPlugin
                        )
                    )
                ]
            ):
                time.sleep(1)
                return SignalNoOp()

            return SignalServicesStop()
        except Exception as e:
            self._logger.error("========== Exception at execution ==========")
            self._logger.error(traceback.format_exc())
            self._logger.error("============================================")

            try:
                self._schedule(
                    State.Execution(
                        "_on_error",
                        self._context.get_plugin_instances_by_type(
                            ResourceHandlerPlugin
                        ),
                        source=self.__class__,
                        exception=e.__context__,
                    )
                )
            except Exception as ex:
                self._logger.error(f"Exception at error handling: {ex}")
                self._logger.error(traceback.format_exc())

            self._context.set_exit_code(ExitCodes.StateResourcesDeletionError)

            return SignalError(e)


class StateServiceStart(State):
    """
    This state is responsible to invoke the service start related methods.

    Invoked methods:

    :meth:`pypz.core.specs.plugin.ServicePlugin._on_service_start`

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        try:
            if not self._schedule(
                *[
                    State.Execution("_on_service_start", level)
                    for level in self._context.get_dependency_graph_by_type(
                        ServicePlugin
                    )
                ],
                break_on_exception=True,
            ):
                time.sleep(1)
                return SignalNoOp()

            if ExecutionMode.ResourceDeletionOnly == self._context.get_execution_mode():
                return SignalResourcesDeletion()

            return SignalResourcesCreation()
        except Exception as e:
            self._logger.error("========== Exception at execution ==========")
            self._logger.error(traceback.format_exc())
            self._logger.error("============================================")

            try:
                self._schedule(
                    State.Execution(
                        "_on_error",
                        self._context.get_plugin_instances_by_type(ServicePlugin),
                        source=self.__class__,
                        exception=e.__context__,
                    )
                )
            except Exception as ex:
                self._logger.error(f"Exception at error handling: {ex}")
                self._logger.error(traceback.format_exc())

            self._context.set_exit_code(ExitCodes.StateServiceStartError)

            return SignalError(e)


class StateServiceShutdown(State):
    """
    This state is responsible to invoke the service shutdown related methods.

    Invoked methods:

    :meth:`pypz.core.specs.plugin.ServicePlugin._on_service_shutdown`

    :param context: :class:`pypz.executors.operator.context.ExecutionContext`
    """

    def __init__(self, context: ExecutionContext, *args, **kwargs):
        super().__init__(context, *args, **kwargs)

    def on_entry(self) -> None:
        pass

    def on_exit(self) -> None:
        pass

    def on_execute(self) -> BaseSignal:
        try:
            if not self._schedule(
                *[
                    State.Execution("_on_service_shutdown", level)
                    for level in reversed(
                        self._context.get_dependency_graph_by_type(ServicePlugin)
                    )
                ]
            ):
                time.sleep(1)
                return SignalNoOp()

            return SignalKill()
        except Exception as e:
            self._logger.error("========== Exception at execution ==========")
            self._logger.error(traceback.format_exc())
            self._logger.error("============================================")

            try:
                self._schedule(
                    State.Execution(
                        "_on_error",
                        self._context.get_plugin_instances_by_type(ServicePlugin),
                        source=self.__class__,
                        exception=e.__context__,
                    )
                )
            except Exception as ex:
                self._logger.error(f"Exception at error handling: {ex}")
                self._logger.error(traceback.format_exc())

            self._context.set_exit_code(ExitCodes.StateServiceShutdownError)

            return SignalKill()
