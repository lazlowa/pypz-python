.. _parameters:

Parameters
==========

Operators and plugins may define parameters to allow it's users to tune the functionality.
As matter of fact, parameters can be set for each :ref:`instance <instances>` via the following method:

.. automethod:: pypz.core.specs.instance.Instance.set_parameter

In the following section it will be explained:

- how an instance can define expected parameters
- how parameters can be cascaded through multiple level of nested instances
- how template interpolation is working

Expected parameters
-------------------

Your instance implementation can expect parameters either as required or as optional.
For this a descriptor class is used:

.. autoclass:: pypz.core.commons.parameters.ExpectedParameter

As you can see, the expected parameters shall be defined both as class and instance variable, where
the class variable has the type of the descriptor class and the instance variable the actual
parameter value.

.. note::
   **Only the execution context will check required parameters automatically.** You can
   invoke instance methods outside of the executor without providing required parameters.
   However, you can always check manually, if all the required parameters have been set by the following method...

   .. automethod:: pypz.core.specs.instance.Instance.get_expected_parameters

   ... by looping through all the expected parameters, where `required = True` and check, if there is already
   a parameter set with that name via:

   .. automethod:: pypz.core.specs.instance.Instance.has_parameter

.. note::
   Note that although you can define an initial value for the required parameters, a value setting
   will still be required.

Cascading parameters
--------------------

Imagine the case, where you have a pipeline with 5 operators, each with 1 input port and 1 output port plugin.
To specify the parameter 'channelLocation' (see :ref:`data_transfer`) you would need to set the same value
for 10 different instances. To avoid such tedious thing, *pypz* provides a feature to cascade parameters from
higher contexts. Sticking to the example, you can set the parameter 'channelLocation' on pipeline level for
all existing plugins in the pipeline like this:

.. code-block:: python

   pipeline.set_parameter(">>channelLocation", "valid_url")

Template parameters
-------------------
