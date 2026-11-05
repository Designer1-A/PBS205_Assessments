# This file has small functions used by the other Python files.
# Keeping the connection code here means we do not have to repeat it.

import json
import pika
import uuid

# RabbitMQ is running on this computer.
RABBITMQ_HOST = "localhost"

# These are the exchange names used in the assessment.
POSITION_EXCHANGE = "position"
QUERY_EXCHANGE = "query"
RESPONSE_EXCHANGE = "query-response"


def connect_to_rabbitmq():
    # Open a connection to RabbitMQ.
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    return connection


def set_up_exchanges(channel):
    # A topic exchange lets us send messages using routing keys.
    channel.exchange_declare(
        exchange=POSITION_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        exchange=QUERY_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        exchange=RESPONSE_EXCHANGE, exchange_type="topic", durable=True
    )


def send_message(channel, exchange, routing_key, message):
    # Convert a Python dictionary into JSON before sending it.
    message_text = json.dumps(message)
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=message_text.encode("utf-8")
    )


def make_request_id():
    # A unique ID helps the query program identify its own response.
    return str(uuid.uuid4())
