# =============================================================================
# Copyright (c) 2024 by Laszlo Anka. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================
from amqp import Connection, Message

from pypz.amqp_io.channels_bkp2 import AMQPChannelWriter
from pypz.core.specs.misc import BlankInputPortPlugin, BlankOutputPortPlugin

cw = AMQPChannelWriter(channel_name="test",
                       context=BlankOutputPortPlugin("writer", schema=""))

cw.set_location("localhost:5672")
print(cw.invoke_resource_creation())
print(cw.invoke_open_channel())
cw.invoke_write_records(["lalo", "lalo1", "lalo2", "lalo3", "lalo4"])
input()
print(cw.invoke_close_channel())
print(cw.invoke_resource_deletion())

# cnx = Connection()
# cnx.connect()
# ch = cnx.channel()
# # ch.basic_qos(prefetch_size=0, prefetch_count=10, a_global=True)
# ch.open()
# ch.queue_declare("test", passive=False, durable=True, exclusive=False, auto_delete=False)
# ch.queue_declare("test2", passive=False, durable=True, exclusive=False, auto_delete=False)
# ch.exchange_declare("tests", type="fanout", durable=True, auto_delete="False")
#
# ch.queue_bind(queue="test", exchange="tests")
# ch.queue_bind(queue="test2", exchange="tests")
#
# ch.basic_publish(Message("lallooop2"), exchange="tests")
#
# ch.close()
# cnx.close()
