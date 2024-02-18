.. _parameters:

Parameters
==========

Operators and plugins may define parameters to allow the users to tune their functionalities.
As matter of fact, parameters can be set for each :class:`Instance <pypz.core.specs.instance.Instance>`
via the following method:

:meth:`pypz.core.specs.instance.Instance.set_parameter`

In the following section it will be explained:

- how an instance can define expected parameters
- how parameters can be cascaded through multiple level of nested instances
- how template interpolation is working

.. _expected_parameters:

Expected Parameters
-------------------

Your instance implementation can expect parameters either as required or as optional.
The :class:`ExpectedParameter <pypz.core.commons.parameters.ExpectedParameter>` descriptor class is used
for this purpose.

Example:

.. code-block:: python

   class TestImpl(Instance):
       required_param = ExpectedParameter(required=True, parameter_type=str)
       optional_param = ExpectedParameter(required=False, parameter_type=str)

       def __init__(self):
           self.required_param = None
           self.optional_param = "defaultValue"

This is equivalent to:

.. code-block:: python

   class TestImpl(Instance):
       required_param = RequiredParameter(str)
       optional_param = OptionalParameter(str)

       def __init__(self):
           self.required_param = None
           self.optional_param = "defaultValue"

As you can see, the expected parameters shall be defined both as class and instance variable, where
the class variable has the type of the descriptor class and the instance variable the actual
parameter value.

.. note::
   **Only the execution context will check required parameters automatically.** You can
   invoke instance methods outside of the executor without providing required parameters.
   However, you can always check manually, if all the required parameters have been set by the following method...

   :meth:`pypz.core.specs.instance.Instance.get_expected_parameters`

   ... by looping through all the expected parameters, where `required = True` and check, if there is already
   a parameter set with that name via:

   :meth:`pypz.core.specs.instance.Instance.has_parameter`

.. note::
   Note that although you can define an initial value for the required parameters, a value setting
   will still be required.

This descriptor class provides the following features:

- ensures type conformity
- provides alternative name for parameters, which can differ from the variable's name
- provides callback, if the value has been updated
- bound to :class:`Instance's <pypz.core.specs.instance.Instance>` parameter store, hence if you
  define a parameter this way, you can access as well via ``instance.get_parameter()`` method

.. _cascading_parameters:

Cascading Parameters
--------------------

Imagine the case, where you have a pipeline with 5 operators, each with 1 input port and 1 output port plugin.
To specify the parameter 'channelLocation' (see :ref:`data_transfer`) you would need to set the same value
for 10 different instances. To avoid such tedious thing, *pypz* provides a feature to cascade parameters from
higher contexts. Sticking to the example, you can set the parameter 'channelLocation' on pipeline level for
all existing plugins in the pipeline like this:

.. code-block:: python

   pipeline.set_parameter(">>channelLocation", "valid_url")

There are two types of cascading parameters:

- **including**, if the parameter name is prefixed by "`#`", then the parameter will not just be cascaded,
  but will be applied to the instance, where it was set
- **excluding**, if the parameter name is prefixed by "`>`", then the parameter will be cascaded,
  but it will not be applied to the instance, where it was set

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
   operator.set_parameter("#param", "value")

   # Set on plugin level
   operator.set_parameter(">param", "value")

   # Set on operator and plugin level
   pipeline.set_parameter(">#param", "value")

   # Set on pipeline and plugin level
   pipeline.set_parameter("#>param", "value")

Template Parameters
-------------------

Imagine the case, where your operator has to access secured resources and it needs the appropriate
credentials. Obviously, there are options like using some credential store solutions e.g., key-vaults
or using third-party libs. However, *pypz* provides a template interpolation feature for parameters.

Based on the time, when the templates are interpolated, there are two different templating syntax.

Instance Time Templates
+++++++++++++++++++++++

Syntax: `${}`

.. code-block:: python

   # Set on pipeline and operator level
   instance.set_parameter("secret", "${env:SECRET}")

In this case the template is resolved as soon as the parameter is set on the instance via the
:meth:`set_parameter() <pypz.core.specs.instance.Instance.set_parameter>` method. This can be used,
if you deploy a pipeline from your system, where you can set the value for the environment variable.

.. warning::
   **Since the template is interpreted already on the instance object the secret
   will be visible in the serialized configuration!** If you want to prevent this, then you
   should use execution time templates described in the next section.

Execution Time Templates
++++++++++++++++++++++++

Syntax: `$()`

.. code-block:: python

   # Set on pipeline and operator level
   instance.set_parameter("secret", "$(env:SECRET)")

In this case the template is resolved by the executor itself i.e., on the machine, where the executor
is started. This requires the capability to control the environment variables on that machine. Unlike
in case of the instance time template parameters, the value of the execution time template is **not
visible** in the serialized configuration.

-----------------------------------------------------------------------------------------------------------

.. note::
   Note that currently only environment variables can be resolved. It is planned to extend this feature
   to resolve files and remote locations as well in the future.