# This program asks the tracker for a person's contact history.
# It prints the contacts from newest to oldest.

import argparse
import json
import time
from common import (
    connect_to_rabbitmq,
    set_up_exchanges,
    send_message,
    make_request_id
)


def main():
    parser = argparse.ArgumentParser(description="Search contact history.")
    parser.add_argument("person_id", help="Person to search for")
    args = parser.parse_args()

    connection = connect_to_rabbitmq()
    channel = connection.channel()
    set_up_exchanges(channel)

    # Make a temporary queue to receive this query's response.
    result = channel.queue_declare(queue="", exclusive=True, auto_delete=True)
    response_queue = result.method.queue
    channel.queue_bind(
        queue=response_queue,
        exchange="query-response",
        routing_key="query.response"
    )

    request_id = make_request_id()
    response_messages = []

    # Save a response only if it has the ID for this query.
    def receive_response(ch, method, properties, body):
        try:
            response = json.loads(body.decode("utf-8"))
            if response.get("request_id") == request_id:
                response_messages.append(response)
        except (ValueError, json.JSONDecodeError):
            print("Received an invalid response.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=response_queue,
        on_message_callback=receive_response
    )

    # Send the query to the tracker.
    query_message = {
        "person_id": args.person_id,
        "request_id": request_id
    }
    send_message(channel, "query", "query.request", query_message)
    print(f"Looking up contacts for {args.person_id}...")

    # Wait a few seconds for a response.
    end_time = time.time() + 5
    try:
        while time.time() < end_time and not response_messages:
            connection.process_data_events(time_limit=0.2)

        if not response_messages:
            print("No response. Check that tracker.py is running.")
            return

        contacts = response_messages[0].get("contacts", [])
        if not contacts:
            print("No contacts recorded.")
        else:
            print(f"Contacts for {args.person_id}, newest first:")
            for contact in contacts:
                time_text = time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(contact["timestamp"])
                )
                print(
                    f"- {contact['person_id']} at "
                    f"({contact['x']}, {contact['y']}) on {time_text}"
                )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
