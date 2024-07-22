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
from typing import Optional, Any

from amqp import Connection, Channel, Message, NotFound

WriterStatusQueueNameExtension = ".writer.status"
ReaderStatusQueueNameExtension = ".reader.status"
MaxStatusMessageRetrieveCount = 100


def is_queue_existing(queue_name: str, channel: Channel):
    try:
        channel.queue_declare(queue=queue_name, passive=True)
        return True
    except NotFound:
        return False


def is_exchange_existing(exchange_name: str, exchange_type: str, channel: Channel):
    try:
        channel.exchange_declare(exchange=exchange_name, type=exchange_type, passive=True)
        return True
    except NotFound:
        return False


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

        self._max_poll_record: int = max_poll_record

        self._subscriptions: set[str] = set()

        self._consumer_name: str = consumer_name

        self._retrieved_data_messages: Queue = Queue()
        """
        This list stores the messages pushed by the server. Its size shall never exceed
        the max_poll_records configuration.
        """

    def _on_message_received(self, message: Message) -> None:
        self._retrieved_data_messages.put(message)

    def subscribe(self, queue_name: str, arguments: Any = None) -> None:
        if queue_name in self._subscriptions:
            raise AttributeError(f"Queue already subscribed: {queue_name}")

        self._channel.basic_consume(
            queue_name, consumer_tag=f"{self._consumer_name}-{queue_name}",
            callback=self._on_message_received,
            arguments=arguments
        )
        self._subscriptions.add(queue_name)

    def unsubscribe(self, queue_name: str) -> None:
        if queue_name not in self._subscriptions:
            raise KeyError(f"Queue not subscribed: {queue_name}")
        self._channel.basic_cancel(consumer_tag=f"{self._consumer_name}-{queue_name}")

    def has_records(self) -> bool:
        if 0 == len(self._subscriptions):
            raise AttributeError("Missing queue subscription, call subscribe() first")

        return (0 < self.get_available_record_count()) or (0 < self._retrieved_data_messages.qsize())

    def get_available_record_count(self) -> int:
        if 0 == len(self._subscriptions):
            raise AttributeError("Missing queue subscription, call subscribe() first")

        record_count = 0
        for queue in self._subscriptions:
            queue_data = self._channel.queue_declare(queue, passive=True)
            record_count += queue_data.message_count

        return record_count

    def poll(self, timeout: Optional[float] = 0) -> list[str]:
        if 0 == len(self._subscriptions):
            raise AttributeError("Missing queue subscription, call subscribe() first")

        try:
            """ We need to acquire either the max number of messages or, if there
                are no more messages, then the timeout will make sure to terminate
                the loop. Notice that drain_events() will retrieve arbitrary number
                of messages in one go instead of all available. """
            while self._max_poll_record > self._retrieved_data_messages.qsize():
                self._connection.drain_events(timeout=timeout)
        except TimeoutError:
            """ After timeout expires, a TimeoutError is raised, which
                is in our case a normal condition, therefor we ignore it. """
            pass

        retrieved_messages = []
        while 0 < self._retrieved_data_messages.qsize():
            message = self._retrieved_data_messages.get()
            retrieved_messages.append(message.body)
        return retrieved_messages

    def commit_messages(self):
        if 0 == len(self._subscriptions):
            raise AttributeError("Missing queue subscription, call subscribe() first")

        self._channel.basic_ack(delivery_tag=0, multiple=True)


class MessageProducer(_MessagingBase):
    def __init__(self, connection: Optional[Connection] = None, *args, **kwargs):
        super().__init__(connection, *args, **kwargs)

    def publish(self, message: str, queue_name: str = "", exchange_name: str = ""):
        self._channel.basic_publish(
            Message(message), mandatory=True, exchange=exchange_name, routing_key=queue_name
        )
