# The tracker receives position updates and stores contact history.
# It also listens for requests from query.py and sends back the results.

import json
import time
from collections import defaultdict
from common import connect_to_rabbitmq, set_up_exchanges, send_message

# Save each person's latest known position.
latest_positions = {}

# Save contact events for each person.
contact_history = defaultdict(list)


def receive_position(channel, method, properties, body):
    # Read the JSON message sent by person.py.
    try:
        position = json.loads(body.decode("utf-8"))
        person_id = str(position["person_id"])
        x = int(position["x"])
        y = int(position["y"])
        timestamp = float(position.get("timestamp", time.time()))
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        print("Ignored an invalid position message.")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Compare this position with the latest position of every other person.
    for other_id, other_position in list(latest_positions.items()):
        same_square = (
            other_id != person_id
            and other_position["x"] == x
            and other_position["y"] == y
        )

        if same_square:
            # Record the contact for both people.
            contact_time = max(timestamp, other_position["timestamp"])
            contact = {
                "person_id": other_id,
                "timestamp": contact_time,
                "x": x,
                "y": y
            }
            contact_history[person_id].append(contact)

            # Save the same event from the other person's point of view.
            other_contact = {
                "person_id": person_id,
                "timestamp": contact_time,
                "x": x,
                "y": y
            }
            contact_history[other_id].append(other_contact)

            print(f"CONTACT: {person_id} met {other_id} at ({x}, {y})")

    # Update the person's latest position after checking for contacts.
    latest_positions[person_id] = {
        "x": x,
        "y": y,
        "timestamp": timestamp
    }
    channel.basic_ack(delivery_tag=method.delivery_tag)


def receive_query(channel, method, properties, body):
    # A query asks for one person's contact history.
    try:
        request = json.loads(body.decode("utf-8"))
        person_id = str(request["person_id"])
        request_id = request["request_id"]

        # Put the newest contact at the top of the results.
        contacts = sorted(
            contact_history.get(person_id, []),
            key=lambda item: item["timestamp"],
            reverse=True
        )

        # Send the result back through the response exchange.
        response = {
            "person_id": person_id,
            "request_id": request_id,
            "contacts": contacts
        }
        send_message(channel, "query-response", "query.response", response)
        print(f"Answered query for {person_id}.")
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        print("Ignored an invalid query message.")
    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    set_up_exchanges(channel)

    # Create queues and connect them to the exchanges.
    channel.queue_declare(queue="tracker.positions")
    channel.queue_bind(
        queue="tracker.positions",
        exchange="position",
        routing_key="position.#"
    )

    channel.queue_declare(queue="tracker.queries")
    channel.queue_bind(
        queue="tracker.queries",
        exchange="query",
        routing_key="query.#"
    )

    # Tell RabbitMQ which functions should handle each kind of message.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue="tracker.positions",
        on_message_callback=receive_position
    )
    channel.basic_consume(
        queue="tracker.queries",
        on_message_callback=receive_query
    )

    print("Tracker is running. Press Ctrl+C to stop.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Tracker stopped.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
