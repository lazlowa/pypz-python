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

!!!Fig - Inheritance model

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

Generic type
++++++++++++

The Instance class is a generic class, where the generic attribute describes the expected type of
the nested instances. It is important, since at construction time all attributes will be scanned
and if one of them has the type of the expected nested instance type, then it will be automatically
identified as a nested instance.

Metaclass
+++++++++

Notice that a custom metaclass is used to be able to intercept the creation of the Instance object.
This concept together with altering the Instance class' __setattr__ method allows amongst more
to dynamically derive the instance's name from the actual variable's name.

!!! Link to classes

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

!!! Link get_dto()

To deserialize and to create an Instance object, you can use

!!! Link create_from_string()

You can find an example `here <https://github.com/lazlowa/pypz-examples>`_

Pipeline
--------

Operator
--------

Plugins
-------

