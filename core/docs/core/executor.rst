.. _executor:

Executor
========

Since the operator entity in *pypz* is self-contained, an executor is required, which controls the execution
flow of the entire operator incl. plugins. This is achieved through a simple state machine.

State machine
-------------

.. graphviz::
   :name: Operator State Machine

   digraph state_machine {
       fontsize=34
       fontcolor=blue

       start [shape=circle, label="", style=filled, color=black, width=0.2, height=0.2]
       end [shape=circle, label="", width=0.2, height=0.2, style=filled, color=black, fillcolor=black, peripheries=2]
       entry [shape=rect, label="StateEntry", style=rounded, width=2, height=0.6]
       killed [shape=rect, label="StateKilled", style=rounded, width=2, height=0.6]
       service_start [shape=rect, label="StateServiceStart", style=rounded, width=2, height=0.6]
       service_shutdown [shape=rect, label="StateServiceShutdown", style=rounded, width=2, height=0.6]
       resource_creation [shape=rect, label="StateResourceCreation", style=rounded, width=2, height=0.6]
       resource_deletion [shape=rect, label="StateResourceDeletion", style=rounded, width=2, height=0.6]
       operation_init [shape=rect, label="StateOperationInit", style=rounded, width=2, height=0.6]
       operation_running [shape=rect, label="StateOperationRunning", style=rounded, width=2, height=0.6]
       operation_shutdown [shape=rect, label="StateOperationShutdown", style=rounded, width=2, height=0.6]

       start -> entry

       entry -> service_start [label="SignalServiceStart"]
       entry -> killed [label="SignalShutdown"]

       service_start -> resource_creation [label="SignalResourcesCreation"]
       service_start -> resource_deletion [label="SignalResourcesDeletion"]
       service_start -> service_shutdown [label="SignalError"]
       service_start -> service_shutdown [label="SignalShutdown"]

       resource_creation -> operation_init [label="SignalOperationInit"]
       resource_creation -> resource_deletion [label="SignalError"]
       resource_creation -> service_shutdown [label="SignalServicesStop"]
       resource_creation -> resource_deletion [label="SignalShutdown"]

       operation_init -> operation_running [label="SignalOperationStart"]
       operation_init -> operation_shutdown [label="SignalError"]
       operation_init -> operation_shutdown [label="SignalShutdown"]

       operation_running -> operation_shutdown [label="SignalOperationStop"]
       operation_running -> operation_shutdown [label="SignalError"]
       operation_running -> operation_shutdown [label="SignalShutdown"]

       operation_shutdown -> resource_deletion [label="SignalResourcesDeletion"]
       operation_shutdown -> service_shutdown [label="SignalServicesStop"]
       operation_shutdown -> resource_deletion [label="SignalError"]

       resource_deletion -> service_shutdown [label="SignalServicesStop"]
       resource_deletion -> service_shutdown [label="SignalError"]

       service_shutdown -> killed [label="SignalKill"]
       service_shutdown -> killed [label="SignalError"]

       killed -> end
   }

Execution modes
---------------
