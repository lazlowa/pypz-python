Models
======

There is a multi-layered design behind *pypz*. It means that on the highest level there is
the pipeline, which contains the operators one level deeper with the actual business logic.
It is possible to enhance the functionalities of the operators via plugins e.g., the port plugins by
which the operators can transfer data between each other or the logger plugins that enables the
operator to send logs to the specified targets. The plugins are on the lowest level.

!!!Fig - basic diagram about the pipelines

Instances
---------

Each runtime entities in *pypz* (pipelines, operators, plugins) are represented as instances.
The blueprints of the instances are called specs and are represented as classes in the code.
In other words, you can consider the specs as the classes and the instances as the objects
created from the classes. This design enables *pypz* to model the pipelines as code.
The Instance class itself is the base for every other specs. It contains all the necessary
logic and feature that is required by the mentioned design.

.. _inheritance_diagram:

.. inheritance-diagram::
   pypz.core.specs.pipeline.Pipeline
   pypz.core.specs.operator.Operator
   pypz.core.specs.plugin.LoggerPlugin
   pypz.core.specs.plugin.ExtendedPlugin
   pypz.core.specs.plugin.ServicePlugin
   pypz.core.specs.plugin.InputPortPlugin
   pypz.core.specs.plugin.OutputPortPlugin
   :parts: 1
   :caption: Inheritance diagram

The instance class is designed in a way that it could be used on its own. However, the Pipeline, Operator and
Plugin classes are specializing and somewhat restricting the base Instance class in a way that is
necessary for *pypz*.

Attributes
++++++++++

Basically an instance has the following attributes:

- **name** (required), which identifies the instance in runtime
- parameters
- **dependencies** to other instances
- **nested instances** i.e., other instances attached to the current instance's context e.g., an operator is a nested instance to a pipeline
- **context instance** i.e., the parent instance e.g., the pipeline is the context of an operator

.. note::
   The name of the instance can be provided either as constructor argument or if it has a context and
   the argument is omitted, then the name of instance will be the name of the variable.

Generic Type
++++++++++++

The Instance class is a generic class, where the generic attribute describes the expected type of
the nested instances. It is important, since at construction time all attributes will be scanned
and if one of them has the type of the expected nested instance type, then it will be automatically
identified as a nested instance.

Metaclass
+++++++++

Notice that a custom metaclass is used to be able to intercept the creation of the Instance object.

.. autoclass:: pypz.core.specs.instance.InterceptedInstance

Dependencies
++++++++++++

It is possible to define dependencies between Instances at runtime. However, you should be aware
that the meaning of the dependencies are defined by the runtime context i.e., just by defining
them has no effect. For example, the operator executor uses the dependency definitions of the plugins to
build a call order list, which then will determine, what plugin runs when. Although not yet implemented,
but a deployer might use the dependency definitions across operators to decide, which operator
is to be deployed at what time.

Parameters
++++++++++

You can define expected parameters for your Instance spec. You can expect both required and
optional parameters to be set for your instance. Check the :ref:`parameters section <parameters>`
for a more detailed explanation.

Data Transfer Object
++++++++++++++++++++

If you want to deploy your pipelines remotely, it is necessary to transfer all the information
about the Instance i.e., the entire Instance with all its nested Instances shall be serialized
before sending and deserialized after receiving and before creating the actual Instance object
again. Since the amount of data for a pipeline is not huge and since we want to ensure that
the serialized format of an Instance is human-readable, we are using YAML.

To serialize an Instance, you can use

.. automethod:: pypz.core.specs.instance.Instance.get_dto

To deserialize and to create an Instance object, you can use

.. automethod:: pypz.core.specs.instance.Instance.create_from_dto

You can find an example `here <https://github.com/lazlowa/pypz-examples>`_

Instance Groups
+++++++++++++++

Instances can be grouped together. For example, if you :ref:`replicate <operator_replication>` an operator, then
you are creating a group of operators. The class InstanceGroup provides the methods to access useful
group related information.

.. autoclass:: pypz.core.specs.instance.InstanceGroup

Pipeline
--------

As its name suggests, the Pipeline class represents a pipeline in *pypz*. Its nested type is
defined as the Operator class. Basically it does not alter or extend the functionality of the
Instance class, since unlike the operators and plugins, the pipeline is a virtual organization
of the Operators.

You can define your operators in the constructor along with some intentionally static configuration
like the :ref:`connection <operator_connection>` between operators.

Operator
--------

The Operator class represents the operator in *pypz*. Its nested type is defined as the Plugin class.
Operators are the most important model, since it contains the implementation of the business logic.
In addition to the Instance class, the Operator class provides the following features:

- :ref:`operator connection <operator_connection>`
- :ref:`operator replication <operator_replication>`
- :ref:`methods <operator_methods>` to implement the business logic
- :ref:`operator logging <operator_logging>`

Expected Parameters
+++++++++++++++++++

A more detailed explanation about the expected parameters can be found in :ref:`parameters section <parameters>`.
This section presents only the expected parameters of the Operator instances.

*operatorImageName*
~~~~~~~~~~~~~~~~~~~

The name of the Docker image, where you included the implemented Operator. This will mainly be used by the
container related deployers.

*replicationFactor*
~~~~~~~~~~~~~~~~~~~

This value will determine, how many replicas shall be created from an operator.

Check `operator replication <operator_replication>` for more information.

.. _operator_connection:

Connections
+++++++++++

You can connect operators by the so called :ref:`port plugins <port_plugins>`.

!!! Fig - operators with port plugins

The connection can be defined on pipeline level, where you shall specify, which operator's which port plugin
shall be connected to which other operator's which port plugin.

!!! Fig - code block

Check :ref:`data_transfer` for more details.

.. _operator_replication:

Replication
+++++++++++

Let's take the following pipeline as example:

!!!Fig - pipeline FileReader Processor Aggregator

The first operator reads the files from some share, the second operator performs
some KPI extraction and the third operator aggregates the results and stores
in some database.

If there is 1 million of files and the KPI extraction takes 1 second for each
file then it takes ~11.5 days to go through all the files. This is why *pypz*
provides the feature of operator replication. This enables selective scaling
in the pipeline. For example, let's replicate the KPI extractor operator 99
times, which will result in total 100 operator (original + replicas):

!!! Fig - replicated operator in pipeline

The output of the FileReader operator will distribute the files evenly across
all the processor operator, hence those will share the load, which will result
appr. 100x faster execution. This results ~2.7 hours for the entire set instead
of 11.5 days.

Based on your capacities, you can go even higher with the replication to boost
even more the processing time.

Replication at its core means that a *pypz* creates an Instance based on the following rules:

- the replica instance has the same type (class) as the original
- the name of the instance contains the replication index i.e., if the original operator's name is 'inst' then the first replica's name is 'inst_0'
- to ensure consistency across instances all the attributes of the original operator is shared with the replicas like parameters, connections, etc.

.. note::
   Note that you are able to set the replication factor i.e., the number of replicas, where
   replication factor = 0 means no replication, only the original.

.. _operator_methods:

Methods
+++++++

The Operator class provides methods to implement your business logic. Each method
is called in specific order based on the :ref:`executor <executor>` implementation.

.. automethod:: pypz.core.specs.operator.Operator._on_init

.. automethod:: pypz.core.specs.operator.Operator._on_running

.. automethod:: pypz.core.specs.operator.Operator._on_shutdown

.. _operator_logging:

Logging
+++++++

Although the logging functionality is provided by the LoggerPlugin class,
the Operator class provides an aggregated logging interface, which means that should there be
multiple LoggerPlugin in an Operator context, with a single method call all LoggerPlugins' corresponding
method will be called in the background:

.. code-block:: python

   self.get_logger().info("Text to log")

The aggregated logger is realized by:

.. autoclass:: pypz.core.specs.operator.Operator.Logger

Check the :ref:`logging` section for more information.

.. _plugins:

Plugins
-------

Plugins are the lowest level entities in *pypz*. They allow to extend an enhance the functionality
of an operator. As you can check on the :ref:`inheritance_diagram`,
there are several plugin interfaces with different purposes.

.. autoclass:: pypz.core.specs.plugin.Plugin
.. autoclass:: pypz.core.specs.plugin.ResourceHandlerPlugin

.. _port_plugins:

.. autoclass:: pypz.core.specs.plugin.PortPlugin
.. autoclass:: pypz.core.specs.plugin.InputPortPlugin
.. autoclass:: pypz.core.specs.plugin.OutputPortPlugin
.. autoclass:: pypz.core.specs.plugin.ServicePlugin
.. autoclass:: pypz.core.specs.plugin.ExtendedPlugin
.. autoclass:: pypz.core.specs.plugin.LoggerPlugin

