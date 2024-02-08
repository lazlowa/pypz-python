.. _parameters:

Parameters
==========

Operators and plugins may define parameters to allow it's users to tune the functionality.
As matter of fact, parameters can be set for each :ref:`instance <instances>` via the following method:

.. automethod:: pypz.core.specs.instance.Instance.set_parameter
   :no-index:

In the following section it will be explained:

- how an instance can define expected parameters
- how parameters can be cascaded through multiple level of nested instances
- how template interpolation is working

.. _expected_parameters:

Expected parameters
-------------------

Your instance implementation can expect parameters either as required or as optional.
For this a descriptor class is used:

.. autoclass:: pypz.core.commons.parameters.ExpectedParameter
   :no-index:

   .. automethod:: __init__
      :no-index:

As you can see, the expected parameters shall be defined both as class and instance variable, where
the class variable has the type of the descriptor class and the instance variable the actual
parameter value.

.. note::
   **Only the execution context will check required parameters automatically.** You can
   invoke instance methods outside of the executor without providing required parameters.
   However, you can always check manually, if all the required parameters have been set by the following method...

   .. automethod:: pypz.core.specs.instance.Instance.get_expected_parameters
      :no-index:

   ... by looping through all the expected parameters, where `required = True` and check, if there is already
   a parameter set with that name via:

   .. automethod:: pypz.core.specs.instance.Instance.has_parameter
      :no-index:

.. note::
   Note that although you can define an initial value for the required parameters, a value setting
   will still be required.

.. _cascading_parameters:

Cascading parameters
--------------------

Imagine the case, where you have a pipeline with 5 operators, each with 1 input port and 1 output port plugin.
To specify the parameter 'channelLocation' (see :ref:`data_transfer`) you would need to set the same value
for 10 different instances. To avoid such tedious thing, *pypz* provides a feature to cascade parameters from
higher contexts. Sticking to the example, you can set the parameter 'channelLocation' on pipeline level for
all existing plugins in the pipeline like this:

.. code-block:: python

   pipeline.set_parameter(">>channelLocation", "valid_url")

There are two types of cascading parameters:

- **including**, if the parameter name is prefixed by "`#`", then the parameter will not just be cascaded, but will be applied on the level, where it was set
- **excluding**, if the parameter name is prefixed by "`>`", then the parameter will be cascaded, but it will not be applied on the level, where it was set

The number of prefixes determines, how many levels will a parameter be cascaded.

The example above is a two-level, excluding cascading parameter i.e., it will be cascaded from pipeline to plugin
level without applying it to either pipeline or operator level.

Examples:

.. code-block:: python

   # Set on pipeline and operator level
   pipeline.set_parameter("#param", "value")

   # Set on pipeline, operator and plugin level
   pipeline.set_parameter("##param", "value")

   # Set on operator and plugin level
   operator.set_parameter("##param", "value")

   # Set on plugin level
   operator.set_parameter(">param", "value")

   # Set on operator and plugin level
   pipeline.set_parameter(">#param", "value")

   # Set on pipeline and plugin level
   pipeline.set_parameter("#>param", "value")

Template parameters
-------------------

Imagine the case, where your operator has to access secured resources and it needs the appropriate
credentials. Obviously, there are options like using some credential store solutions like key-vaults
or using third-party libs. However, *pypz* provides a template interpolation features for parameters.

Based on the time, when the templates are interpolated, there are two different templating syntax.

Instance time templates
+++++++++++++++++++++++

Syntax: `${}`

.. code-block:: python

   # Set on pipeline and operator level
   instance.set_parameter("secret", "${env:SECRET}")

In this case the template is resolved as soon as the parameter is on the instance. This can be used,
if you deploy a pipeline from your system, where you can set the value for the environment variable.
**However, the secret will be visible in the serialized configuration.**

Execution time templates
++++++++++++++++++++++++

Syntax: `$()`

.. code-block:: python

   # Set on pipeline and operator level
   instance.set_parameter("secret", "$(env:SECRET)")

In this case the template is resolved by the executor itself i.e., on the machine, where the executor
is started. This requires the capability to control the environment variables on that machine. Unlike
in case of the instance time template parameters, the value of the execution time template is not
visible in the serialized configuration.

-----------------------------------------------------------------------------------------------------------

.. note::
   Note that currently only environment variables can be resolved. It is planned to extend this feature
   to resolve files and remote locations as well in the future.