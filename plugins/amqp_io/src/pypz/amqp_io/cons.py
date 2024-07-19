import time

from amqp import Connection, Message

from pypz.amqp_io.channels_bkp2 import AMQPChannelReader
from pypz.core.specs.misc import BlankInputPortPlugin

# cw = AMQPChannelReader(channel_name="test",
#                        context=BlankInputPortPlugin("reader", schema=""))
#
# cw.set_location("localhost:5672")
# print(cw.invoke_resource_creation())
# print(cw.invoke_open_channel())
# cw.set_initial_record_offset_auto()
# input()
# print(cw.invoke_read_records())
# cw.invoke_commit_current_read_offset()
# print(cw.invoke_close_channel())
# print(cw.invoke_resource_deletion())


# with Connection(host="localhost:5672") as admin_connection:
#     admin_channel = admin_connection.channel()
#     admin_channel.queue_declare("test-stream",
#                                 passive=False, durable=True, exclusive=False, auto_delete=False,
#                                 arguments={"x-queue-type": "stream"})
#     admin_channel.basic_publish(Message("streaming"), routing_key="test-stream")


with Connection(host="localhost:5672") as admin_connection:
    admin_channel = admin_connection.channel()
    admin_channel.basic_qos(0, 100, False)


    def on_message(message):
        print(message.body)
        admin_channel.basic_ack(message.delivery_tag)


    admin_channel.basic_consume("amqp_pipeline.reader.input_port.reader.status",
                                callback=on_message,
                                arguments={"x-stream-offset": "first"})

    # admin_channel2 = admin_connection.channel()
    # admin_channel2.basic_qos(0, 100, False)
    #
    #
    # def on_message2(message):
    #     print(message.body)
    #     admin_channel2.basic_ack(message.delivery_tag)
    #
    #
    # admin_channel2.basic_consume("test-stream", callback=on_message2,
    #                              arguments={"x-stream-offset": "first"})

    while True:
        try:
            admin_connection.drain_events(timeout=2)
        except TimeoutError:
            pass
        print("read")
