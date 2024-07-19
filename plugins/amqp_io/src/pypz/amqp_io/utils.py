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
from queue import Queue
from typing import Optional

from amqp import Connection, Channel, Message, NotFound


class _MessagingBase:
    def __init__(self,
                 connection: Optional[Connection] = None,
                 *args, **kwargs):
        self._connection: Connection = Connection(*args, **kwargs) if connection is None else connection
        self._connection.connect()

        self._channel: Channel = self._connection.channel()
        """
        Lightweight virtual channel initialized over the physical connection
        """

    def close(self) -> None:
        self._connection.close()

    def is_queue_existing(self, queue_name: str) -> bool:
        try:
            self._channel.queue_declare(queue=queue_name, passive=True)
            return True
        except NotFound:
            return False


class MessageConsumer(_MessagingBase):
    def __init__(self, consumer_name: str, max_poll_record: Optional[int] = 1,
                 connection: Optional[Connection] = None, *args, **kwargs):
        super().__init__(connection, *args, **kwargs)

        self._channel.basic_qos(0, max_poll_record, False)
        """ This setting is required to prevent memory overflow, since
            without this setting, the server would push every new message
            to the consumer regardless, how fast the consumer can process it,
            the prefetch count defines the max number of messages pushed
            by the server before requiring 'acknowledge' signal."""

        self._subscribed_queue: Optional[str] = None

        self._consumer_name: str = consumer_name

        self._retrieved_data_messages: Queue = Queue()
        """
        This list stores the messages pushed by the server. Its size shall never exceed
        the max_poll_records configuration.
        """

        self._data_messages_to_acknowledge: Queue = Queue()
        """
        This list stores the messages returned to the user and hence ready to acknowledge.
        """

    def _on_message_received(self, message: Message) -> None:
        self._retrieved_data_messages.put(message)

    def subscribe(self, queue_name: str) -> None:
        if self._subscribed_queue != queue_name:
            if self._subscribed_queue is not None:
                self._channel.basic_cancel(consumer_tag=f"{self._consumer_name}-{self._subscribed_queue}")

            self._subscribed_queue = queue_name

            self._channel.basic_consume(
                queue_name, consumer_tag=f"{self._consumer_name}-{self._subscribed_queue}",
                callback=self._on_message_received
            )

    def has_records(self) -> bool:
        if self._subscribed_queue is None:
            raise AttributeError("Missing queue subscription, call subscribe() first")

        if 0 < self._retrieved_data_messages.qsize():
            return True

        queue_data = self._channel.queue_declare(self._subscribed_queue, passive=True)
        return 0 < queue_data.message_count

    def poll(self, timeout: Optional[int] = 0) -> list[str]:
        if self._subscribed_queue is None:
            raise AttributeError("Missing queue subscription, call subscribe() first")

        try:
            self._connection.drain_events(timeout=timeout)
        except TimeoutError:
            """ After timeout expires, a TimeoutError is raised, which
                is in our case a normal condition, therefor we ignore it. """
            pass

        retrieved_messages = []
        while 0 < self._retrieved_data_messages.qsize():
            message = self._retrieved_data_messages.get()
            retrieved_messages.append(message.body)
            self._data_messages_to_acknowledge.put(message)
        return retrieved_messages

    def commit_messages(self):
        if self._subscribed_queue is None:
            raise AttributeError("Missing queue subscription, call subscribe() first")

        while 0 < self._data_messages_to_acknowledge.qsize():
            message = self._data_messages_to_acknowledge.get()
            self._channel.basic_ack(message.delivery_tag)


class MessageProducer(_MessagingBase):
    def __init__(self, connection: Optional[Connection] = None, *args, **kwargs):
        super().__init__(connection, *args, **kwargs)

    def publish(self, message: str, queue_name: str):
        self._channel.basic_publish(Message(message), routing_key=queue_name)
