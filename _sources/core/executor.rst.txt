.. _executor:

Executor
========

Since the operator entity in *pypz* is self-contained, an
:class:`OperatorExecutor <pypz.executors.operator.executor.OperatorExecutor>`
is required, which controls the execution flow of the entire operator incl. plugins.
The executor has the following main components:

- :class:`ExecutionContext <pypz.executors.operator.context.ExecutionContext>`
- state machine (embedded into the :class:`OperatorExecutor <pypz.executors.operator.executor.OperatorExecutor>`)

Execution Context
-----------------

The :class:`ExecutionContext <pypz.executors.operator.context.ExecutionContext>` is
responsible to maintain all the necessary information during execution.
It can be forwarded to the states of the state machine as well.

The execution context has the following responsibilities:

- check, if all required parameters are set
- maintain exit code
- register plugins along their types
- traverse plugin dependency graph
- store information about execution mode

State machine
-------------

.. graphviz::
   :name: Operator State Machine
   :align: center

   digraph state_machine {
       size="10,20";
       ratio="auto";
       ranksep="2";
       rankdir="TB";
       margin="0.0,0.5";
       graph [fontname="Verdana", fontsize="28"];

       start [shape=circle, label="", style=filled, color=black, width=0.2, height=0.2];
       end [shape=circle, label="", width=0.2, height=0.2, style=filled, color=black, fillcolor=black, peripheries=2];
       entry [shape=rect, label="StateEntry", style=rounded, width=2, height=0.6];
       killed [shape=rect, label="StateKilled", style=rounded, width=2, height=0.6];
       service_start [shape=rect, label="StateServiceStart", style=rounded, width=2, height=0.6];
       service_shutdown [shape=rect, label="StateServiceShutdown", style=rounded, width=2, height=0.6];
       resource_creation [shape=rect, label="StateResourceCreation", style=rounded, width=2, height=0.6];
       resource_deletion [shape=rect, label="StateResourceDeletion", style=rounded, width=2, height=0.6];
       operation_init [shape=rect, label="StateOperationInit", style=rounded, width=2, height=0.6];
       operation_running [shape=rect, label="StateOperationRunning", style=rounded, width=2, height=0.6];
       operation_shutdown [shape=rect, label="StateOperationShutdown", style=rounded, width=2, height=0.6];

       start -> entry;

       entry -> service_start [label="SignalServiceStart"];
       entry -> killed [label="SignalShutdown"];

       service_start -> resource_creation [label="SignalResourcesCreation"];
       service_start -> resource_deletion [label="SignalResourcesDeletion"];
       service_start -> service_shutdown [label="SignalError"];
       service_start -> service_shutdown [label="SignalShutdown"];

       resource_creation -> operation_init [label="SignalOperationInit"];
       resource_creation -> resource_deletion [label="SignalError"];
       resource_creation -> service_shutdown [label="SignalServicesStop"];
       resource_creation -> resource_deletion [label="SignalShutdown"];

       operation_init -> operation_running [label="SignalOperationStart"];
       operation_init -> operation_shutdown [label="SignalError"];
       operation_init -> operation_shutdown [label="SignalShutdown"];

       operation_running -> operation_shutdown [label="SignalOperationStop"];
       operation_running -> operation_shutdown [label="SignalError"];
       operation_running -> operation_shutdown [label="SignalShutdown"];

       operation_shutdown -> resource_deletion [label="SignalResourcesDeletion"];
       operation_shutdown -> service_shutdown [label="SignalServicesStop"];
       operation_shutdown -> resource_deletion [label="SignalError"];

       resource_deletion -> service_shutdown [label="SignalServicesStop"];
       resource_deletion -> service_shutdown [label="SignalError"];

       service_shutdown -> killed [label="SignalKill"];
       service_shutdown -> killed [label="SignalError"];

       killed -> end;
   }

You can find more information incl. invoked plugin methods in the corresponding
:class:`module <pypz.executors.operator.states>`.

Execution modes
---------------

As you might see on the state machine diagram, there are different routes from start to end.
Which route the state machine will take depends not just on the execution results of the
states, but on the specified execution mode as well.

Imagine the case, where your one or more operators in your pipeline crashed. Resources might
not have been released in this case. Since *pypz* does not know anything about your resources,
only you do, *pypz* cannot provide an integrated feature to clean up your resources. However,
what *pypz* can provide is an execution mode, where only resources will be deleted instead
of running the actual business logic.

Check :class:`pypz.executors.commons.ExecutionMode` for more details.
