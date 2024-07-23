RabbitMQ IO Plugin
==================

Overview
--------

The :class:`RMQChannelInputPort <pypz.plugins.rmq_io.ports.RMQChannelInputPort>` and
:class:`RMQChannelOutputPort <pypz.plugins.rmq_io.ports.RMQChannelOutputPort>` enables
an operator to send and receive data through queues in `RabbitMQ <https://www.rabbitmq.com/>`_.
As described in the :ref:`data_transfer` section, both port plugins integrate the
specific channel implementations: :class:`RMQChannelReader <pypz.plugins.rmq_io.channels.RMQChannelReader>`,
:class:`RMQChannelWriter <pypz.plugins.rmq_io.channels.RMQChannelWriter>`.

rajz writer 3x reader + queue

Queues enables *pypz* to realize actual load distribution instead of "just" data distribution
like Kafka (check the :ref:`appendix` for explanation).

.. note::
   Although this implementation uses the `py-amqp <https://github.com/celery/py-amqp>`_ lib
   (which would make it compatible to all AMQP broker solutions),
   `RabbitMQ Streams <https://www.rabbitmq.com/docs/streams>`_ are utilized
   for the control communication between :class:`ChannelReaders <pypz.core.channels.io.ChannelReader>`
   and :class:`ChannelWriters <pypz.core.channels.io.ChannelWriter>`. Therefore, this implementation
   can be used only with RabbitMQ.

Usage
-----

Plugins shall be defined in the operator's constructor. It is important, since attributes
will be scanned in construction time and plugins defined in the constructor will be register
automatically as nested instance.

.. code-block:: python

   from pypz.core.specs.operator import Operator
   from pypz.plugins.rmq_io.ports import RMQChannelOutputPort, RMQChannelInputPort


   class DemoOperator(Operator):

       def __init__(self, name: str = None, *args, **kwargs):
           super().__init__(name, *args, **kwargs)

           self.input_port = RMQChannelInputPort()
           self.output_port = RMQChannelOutputPort()


.. _appendix:

Appendix
--------

Why bother, if Kafka provides similar functionalities with its consumer groups and partition
assignment coordination?

Kafka addresses a completely different challenge at its core.
It is essentially an append-only event log, which mimics queuing by distributing
records over partitions and assigning consumers in the same group to those partitions.
Ideally, if there is as many consumers as partitions, all of the them start at the same time,
have comparable resources, and none of them crash, then the data transfer indeed resembles queuing.

rajz arról, hogyan csinálja Kafka

However, you should notice that Kafka realizes rather data distribution than load distribution.
The following example shows a case, where this nature of Kafka causes issues.

rajz arról, hogy az egyik consumer elszáll

As you can see, if a consumer crashes, the group coordinator assigns an other consumer to the
lingering partition. However, since only one consumer can be assigned to a partition in a specific
group, that consumer will process all the messages left by the crashing consumer in addition
to the messages of its own partition. In addition, confluent confirmed that they can guarantee
stability only up to 6 consumer in group, which poses additional limitations.



The answer is simple, real load sharing is only possible with real queues. Although
Kafka attempts to provide similar functionalities with its consumer groups and partition
assignment coordination, yet it cannot provide the same performance as a real queueing
system.