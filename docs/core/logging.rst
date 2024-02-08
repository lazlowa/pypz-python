.. _logging:

Logging
=======

Context Logging
---------------

Since *pypz* has a :ref:`multi-layered design <models>` and entities on each layer are existing in their own context,
we want to have a logging concept that is consistent and reusable across contexts. For example,
if a plugin wants to log something, then it would be nice to reuse the logger of the operator, but denoting that
the log came from the plugin's context.

.. code-block:: python

   operator.get_logger().info("something")
   plugin.get_logger().info("something")

.. code-block:: console

   INFO | pipeline | operator - something
   INFO | pipeline | operator | plugin - something

.. note::
   Since a pipeline is a virtual organization of operators, it does not have logger interface, but acts as a
   context for the operators.

So we have the following requirements:

- one logger to rule them all
- context information shall be available based on from where the logger is called

The concept involves the following classes:

.. autoclass:: pypz.core.commons.loggers.ContextLoggerInterface
   :no-index:

.. autoclass:: pypz.core.commons.loggers.ContextLogger
   :no-index:

The `ContextLoggerInterface` shall be used to implement the actual logging functionality. Notice that
this class provides protected methods that shall not be called directly. The `ContextLogger` provides
the functionality to call the methods of `ContextLoggerInterface` so that it supplies the context
information in the background. You will then use a `ContextLogger` object to perform logging.

Operator Logging
----------------

Since an operator is a self-contained entity, we need to ensure that there is only one logger within
an operator context, which can be used by the operator and the nested plugins as well. An additional
twist to the story that there might be multiple logger plugins in an operator. Instead of calling
each logger plugins' corresponding methods manually, we want to abstract this, so for the user it
is one call.

The following diagram shows the inheritance map of the logging-relevant classes:

.. inheritance-diagram::
   pypz.core.specs.plugin.LoggerPlugin
   pypz.core.commons.loggers.ContextLogger
   pypz.plugins.loggers.default.DefaultLoggerPlugin
   :parts: 1
   :caption: Inheritance diagram

Notice that the `LoggerPlugin` interface inherits from the `ContextLoggerInterface`, so if you implement
a logger plugin, then you actually implement the `ContextLoggerInterface`.

Furthermore, there is a `ContextLoggerInterface` implementation in the Operator:

.. autoclass:: pypz.core.specs.operator.Operator.Logger
   :no-index:

This class implements the functionality to abstract the method call of all LoggerPlugins in the operator context.

If you check the Operator's constructor, you will notice the member `self.__logger`. This is actually a `ContextLogger`
object, where the Operator.Logger is provided as logger implementation and the context is the full name of the
operator. This is the one and only logger in the entire operator context. If a plugin is created within this context,
then it creates its own `ContextLogger` object, however as the logger implementation it will take the logger of
the operator, while adding its own context to the context list. Hence, if any logger method will be called inside
the plugin, it will be routed to the operator's `ContextLogger`, which invokes the implemented method of the
Operator.Logger, which will call all the LoggerPlugins' corresponding method.

To access the logger in either the operator or plugin context, you simply need to invoke the ``get_logger()`` method.

.. code-block:: python

   operator.get_logger().info("something")
   plugin.get_logger().info("something")

.. warning::
   Currently there is a guard in Operator.Logger, which will trace back the call stack and will throw an exception
   if a LoggerPlugin tries to invoke any of the logger methods. This will prevent recursion at a cost of some
   CPU load.
