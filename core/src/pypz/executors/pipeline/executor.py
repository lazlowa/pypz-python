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
import concurrent.futures
import signal
from typing import Optional

from pypz.executors.commons import ExecutionMode
from pypz.executors.operator.executor import OperatorExecutor
from pypz.core.specs.pipeline import Pipeline


class PipelineExecutor:
    """
    This class implements the feature to execute an entire pipeline i.e., each operator
    individually via an :class:`OperatorExecutor <pypz.executors.operator.executor.OperatorExecutor>`.
    To increase the performance of the execution, each
    :class:`OperatorExecutor <pypz.executors.operator.executor.OperatorExecutor>` will be launched
    in a separate thread.

    .. warning::
       This class has the sole purpose to allow you to test pipelines locally, it should not be used
       for production workloads, since python is not very optimal for concurrent executions. That is
       the reason to limit the operator count.

    :param pipeline: the pipeline instance to be executed
    """

    _max_operator_count: int = 32
    """
    This value limits the number of operators to be executed parallely. An exception will be
    thrown, if exceeded.
    """

    def __init__(self, pipeline: Pipeline):
        signal.signal(signal.SIGTERM, self.interrupt)
        signal.signal(signal.SIGINT, self.interrupt)

        self.__pipeline: Pipeline = pipeline

        self.__executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

        self.__operator_executors: set[OperatorExecutor] = set()

        self.__futures: set[concurrent.futures.Future] = set()

        """ Creating the OperatorExecutor objects. Notice that none of the OperatorExecutors
            may handle interrupts, since this will be handled on PipelineExecutor level. """
        for operator in self.__pipeline.get_protected().get_nested_instances().values():
            self.__operator_executors.add(OperatorExecutor(operator, handle_interrupts=False))

        if PipelineExecutor._max_operator_count < len(self.__operator_executors):
            raise AttributeError(f"Max number of operators exceeded "
                                 f"({PipelineExecutor._max_operator_count}): "
                                 f"{len(self.__operator_executors)}")

    def start(self, exec_mode: ExecutionMode = ExecutionMode.Standard):
        """
        This method triggers the execution by creating a ``ThreadPoolExecutor`` and submitting
        the :class:`OperatorExecutor <pypz.executors.operator.executor.OperatorExecutor>`'s
        corresponding method.

        :param exec_mode: :class:`pypz.executors.commons.ExecutionMode`
        """

        if self.__executor is None:
            self.__executor = concurrent.futures.ThreadPoolExecutor(max_workers=PipelineExecutor._max_operator_count,
                                                                    thread_name_prefix=self.__class__.__name__)
            self.__futures.clear()
            for operator_executor in self.__operator_executors:
                self.__futures.add(self.__executor.submit(operator_executor.execute, exec_mode))

            while any(not future.done() for future in self.__futures):
                pass

    def shutdown(self):
        """
        This method shuts down the ``ThreadPoolExecutor``. Notice that it blocks until all
        OperatorExecutor has finished. Notice as well that we don't cancel futures, since
        it will be handled upon handling the interrupt signals.
        """

        if self.__executor is not None:
            self.__executor.shutdown(wait=True, cancel_futures=False)
            self.__executor = None

    def interrupt(self, signal_number, current_stack):
        """
        This method is called upon receiving a system signal e.g., SIGINT.
        We are interrupting each :class:`OperatorExecutor <pypz.executors.operator.executor.OperatorExecutor>`
        by invoking :meth:`interrupt() <pypz.executors.operator.executor.OperatorExecutor.interrupt>` only
        if it is still running. Notice that we cancel futures i.e., should an
        :class:`OperatorExecutor <pypz.executors.operator.executor.OperatorExecutor>` not yet be scheduled,
        it will prevent to be scheduled.
        """

        for operator_executor in self.__operator_executors:
            if operator_executor.is_running():
                operator_executor.interrupt(signal_number, current_stack)
        for future in self.__futures:
            future.cancel()
