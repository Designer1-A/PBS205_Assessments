import pika

# Connects to local host 
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost", 5672))
channel = connection.channel()

# Filters requests that meet the requirments
channel.exchange_declare(exchange="test", exchange_type="topic")
result = channel.queue_declare(queue="", exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange="test", queue=queue_name, routing_key="hello.*")

# Receives message
def on_message(ch, method, properties, body):
    print("Received:", body.decode(), "| routing key:", method.routing_key)

# Delete the message once delivered and keep the program running
channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=True)
print("Waiting for messages... (Ctrl+C to stop)")
channel.start_consuming()

# Reference
# https://www.rabbitmq.com/tutorials/tutorial-five-python