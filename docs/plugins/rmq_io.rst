RabbitMQ IO Plugin
==================

Overview
--------

The :class:`RMQChannelInputPort <pypz.plugins.rmq_io.ports.RMQChannelInputPort>` and
:class:`RMQChannelOutputPort <pypz.plugins.rmq_io.ports.RMQChannelOutputPort>` enables
an operator to send and receive data through Kafka. It extends the abstract plugin classes
:class:`ChannelInputPort <pypz.abstracts.channel_ports.ChannelInputPort>` and
:class:`ChannelOutputPort <pypz.abstracts.channel_ports.ChannelOutputPort>` respectively.

As described in the :ref:`data_transfer` section, both
:class:`ChannelInputPort <pypz.abstracts.channel_ports.ChannelInputPort>` and
:class:`ChannelOutputPort <pypz.abstracts.channel_ports.ChannelOutputPort>` integrates
the :class:`ChannelReader <pypz.core.channels.io.ChannelReader>` and
:class:`ChannelWriter <pypz.core.channels.io.ChannelWriter>` abstracts by taking care
of all complexities and boilerplates for you.

Usage
-----

Plugins shall be defined in the operator's constructor. It is important, since attributes
will be scanned in construction time and plugins defined in the constructor will be register
automatically as nested instance.

.. code-block:: python

   from pypz.core.specs.operator import Operator
   from pypz.plugins.kafka_io.ports import KafkaChannelOutputPort, KafkaChannelInputPort


   class DemoOperator(Operator):

       def __init__(self, name: str = None, *args, **kwargs):
           super().__init__(name, *args, **kwargs)

           self.input_port = KafkaChannelInputPort(schema="SCHEMA STRING")
           self.output_port = KafkaChannelOutputPort(schema="SCHEMA STRING")

.. note::
   Note that theoretically it would not be necessary to create a separate class for a channel
   port plugin, since both :class:`ChannelInputPort <pypz.abstracts.channel_ports.ChannelInputPort>` and
   :class:`ChannelOutputPort <pypz.abstracts.channel_ports.ChannelOutputPort>` accepts channel implementations
   as constructor argument, hence the following code is equivalent to the code above:

   .. code-block:: python

      from pypz.core.specs.operator import Operator
      from pypz.abstracts.channel_ports import ChannelInputPort, ChannelOutputPort
      from pypz.plugins.kafka_io.channels import KafkaChannelReader, KafkaChannelWriter


      class DemoOperator(Operator):

          def __init__(self, name: str = None, *args, **kwargs):
              super().__init__(name, *args, **kwargs)

              self.input_port = ChannelInputPort(schema="SCHEMA STRING", channel_reader_type=KafkaChannelReader)
              self.output_port = ChannelOutputPort(schema="SCHEMA STRING", channel_reader_type=KafkaChannelWriter)

Features
--------

On the following diagram you can see, what resources the ChannelReader/-Writer utilizes.

.. raw:: html
   :file: ../resources/htmls/plugins_kafka_io_channels.drawio.html

It is important to note that the :class:`KafkaChannelReader <pypz.plugins.kafka_io.channels.KafkaChannelReader>`
is responsible to create resources. This might seem to be counterintuitive, if you consider a server/client connection,
where the server would be the :class:`KafkaChannelWriter <pypz.plugins.kafka_io.channels.KafkaChannelWriter>`,
hence it should be responsible for the resource creation, however this design simplifies the complexity to
synchronize input and output ports to each other. Synchronization refers to the process, where output ports
shall wait for the input ports to be ready before sending data.

On the following diagram you can see:

- that there will be as many partitions created in a topic as many operators are in a replication group
- how the data consumers in the :class:`KafkaChannelReaders <pypz.plugins.kafka_io.channels.KafkaChannelReader>`
  are directly assigned to a partition based on their replication group index
- consumers in the replication group are forming a consumer group as well from Kafka's perspective
- how the records are sent and distributed to the Kafka topics
- how the records are polled

.. raw:: html
   :file: ../resources/htmls/plugins_kafka_io_transmission.drawio.html

.. important::
   It is **very important** to note that Kafka's group coordinator assigns each consumer to a partition based
   on a certain strategy. However, when there are numerous consumers in a group, the coordinator may experience
   a performance hit during group re-balancing. To address this issue, we are bypassing the group coordinator
   and directly assigning consumers to partitions based on their replication group index. This approach enables us
   to accommodate 100 to 1000s of consumers within the same group.
