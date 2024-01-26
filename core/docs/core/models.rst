Models
======

As usual, there is a multi-layered design behind *pypz*. It means that on the highest level there is
the pipeline, which contains the operators, which contains the actual business logic. However, it
is possible to enhance the functionalities of the operators via plugins e.g., the port plugins by
which the operators can transfer data between each other or the logger plugins that enables the
operator to send logs to the specified targets.

Fig - basic diagram about the pipelines

Instances
---------

Each runtime entities in pypz (pipelines, operators, plugins) are represented as instances.
The blueprints of the instances are called specs and are represented as classes in the code.
In other words, you can consider the specs as the classes and the instances the objects
created from the classes. This design enables *pypz* to model the pipelines as code.
The Instance class itself is the base for every other specs. It contains all the necessary
logic and feature that is required by the mentioned design.

Fig - Inheritance model

The instance class is designed in a way that it could be used on its own, the Pipeline, Operator and
Plugin classes are specializing and somewhat restricting the base Instance class in a way that is
necessary for *pypz*.

Basically an instance has the following attributes:

- name (required), which identifies the instance in runtime
- parameters
- dependencies to other instances
- nested instances i.e., other instances attached to the current instance's context e.g., an operator is a nested instance to a pipeline
- context instance i.e., the parent instance e.g., the pipeline is the context of an operator

.. note::
   The name of the instance can be provided either as constructor argument or if it has a context and
   the argument is omitted, then the name of instance will be the name of the variable.

Operator
--------

Pipeline
--------

Plugins
-------

Working with YAML
-----------------
