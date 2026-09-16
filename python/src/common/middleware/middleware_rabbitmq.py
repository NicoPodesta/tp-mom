import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._host = host
        self._queue_name = queue_name
        self._connection = None
        self._channel = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host)
            )
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name, durable=False)
            self._channel.basic_qos(prefetch_count=1)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error connecting or declaring queue: {e}"
            )

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while sending message: {e}"
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"Unexpected error while sending message: {e}"
            )

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Unexpected error while closing: {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        pass

    def send(self, message):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        pass
