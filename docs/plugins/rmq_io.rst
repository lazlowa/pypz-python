.. _rmq_io_docs:

RabbitMQ IO Plugin
==================

.. _overview:

Overview
--------

The :class:`RMQChannelInputPort <pypz.plugins.rmq_io.ports.RMQChannelInputPort>` and
:class:`RMQChannelOutputPort <pypz.plugins.rmq_io.ports.RMQChannelOutputPort>` enables
an operator to send and receive data through queues in `RabbitMQ <https://www.rabbitmq.com/>`_.
As described in the :ref:`data_transfer` section, both port plugins integrate the
specific channel implementations: :class:`RMQChannelReader <pypz.plugins.rmq_io.channels.RMQChannelReader>`,
:class:`RMQChannelWriter <pypz.plugins.rmq_io.channels.RMQChannelWriter>`.

Using queues enables *pypz* to realize real load distribution (unlike Kafka, which only mimics
queueing, therefore providing "only" data distribution, check the :ref:`load_vs_data_distribution` for explanation).

.. important::
   **When should I use it?**

   You should use RMQ port plugins, if the data records represent a distinct, independent package
   of work to be done. For example, if there is a pipeline, where the first operator loads contents from
   a file from a share, which shall be processed by the next operator and the results shall be aggregated
   by a third operator. In this case, there is no dependency between the data records and each record can
   be translated into a workload to be done.

   **When should I not use it?**

   - if there is no dependency between the data records e.g., time continuous signals
   - where the ordering of the records is important
   - if some kind of replaying, logging ability or append-only behavior is required

   For those cases, you should check :ref:`kafka_io_docs`.

.. note::
   Although this implementation uses the `py-amqp <https://github.com/celery/py-amqp>`_ lib,
   which would make it compatible to all AMQP broker solutions,
   `RabbitMQ Streams <https://www.rabbitmq.com/docs/streams>`_ are utilized
   for the control communication between :class:`RMQChannelReaders <pypz.plugins.rmq_io.channels.RMQChannelReader>`
   and :class:`RMQChannelWriters <pypz.plugins.rmq_io.channels.RMQChannelWriter>`. Therefore, this implementation
   can be used only with RabbitMQ.

Features
--------

On the following diagram you can see, what resources the ChannelReader/-Writer utilizes.

.. raw:: html
   :file: ../resources/htmls/plugins_rmq_io_channels.drawio.html

It is important to note that the :class:`RMQChannelReader <pypz.plugins.rmq_io.channels.RMQChannelReader>`
is responsible to create resources. This might seem to be counterintuitive, if you consider a server/client connection,
where the server would be the :class:`RMQChannelWriter <pypz.plugins.rmq_io.channels.RMQChannelWriter>`,
hence it should be responsible for the resource creation, however this design simplifies the complexity to
synchronize input and output ports to each other. Synchronization refers to the process, where output ports
shall wait for the input ports to be ready before sending data.

On the following diagram, you can see that each reader operator in a replication group shares the load by picking
the next available record from the queue, if they are ready to process. You can see as well that different
operators in different replication groups have a their individual queues. This mechanism isolates the data
processing for each replication groups.

.. raw:: html
   :file: ../resources/htmls/plugins_rmq_io_01.drawio.html

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


       ...

       def _on_running(self):
           self.output_port.send(["message_0", "message_1"])
           messages = self.input_port.retrieve()

       ...

Once the port plugins are constructed, you can send/retrieve data through the usual methods.

.. important::
   Note that as of 08/2024, the RMQ channels can send and retrieved string data i.e.,
   you need to care for serialization and deserialization. At a later time, you will
   be able to specify Avro schema.

.. _load_vs_data_distribution:

Load vs. Data Distribution
--------------------------

As mentioned in the :ref:`overview`, although it is possible to distribute processing
with Kafka, there is a fundamental difference in how and what it distributes
compared to an actual queueing system.

At its core, Kafka addresses a completely different challenge.
It is essentially an append-only event log, which mimics queuing by distributing
records over partitions and assigning consumers in the same group to those partitions.
Ideally, if there is as many consumers as partitions, all of the them start at the same time,
have comparable resources, and none of them crashes, then the data transfer indeed resembles queuing.

.. raw:: html
   :file: ../resources/htmls/plugins_rmq_io_kafka_basics.drawio.html

However, you should notice that Kafka realizes rather data distribution than load distribution.
The following example shows a case, where this nature of Kafka causes issues.

.. raw:: html
   :file: ../resources/htmls/plugins_rmq_io_kafka_issue.drawio.html

As you can see, if a consumer crashes, the group coordinator assigns an other consumer to the
lingering partition. However, since only one consumer can be assigned to a partition in a specific
group, that consumer will process all the messages left by the crashed consumer. In case of
a queue, both remaining consumers would pick the leftover records reducing the overall processing
time.

Additional limitation by Kafka is that according to official confirmation Kafka can guarantee
stability only up to 6 consumer in group. It is not a hard limit, however the more consumer
you have in the group, the longer it takes the group coordinator to properly assign the
partitions. At some point even the stability of the coordination is jeopardized.
