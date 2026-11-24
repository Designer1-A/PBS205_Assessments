"""
history_service.py: remembers the latest messages of every room, saved in SQLite

Start it ONCE, before the chat clients:
    python history_service.py 5672

How it works
  * It listens to every room and saves each message in an SQLite file
  * When a user joins a room, the client sends a request to the queue chat.history.requests.
    This service replies with that room's latest HISTORY_LIMIT messages, starting first with oldest.
"""
import argparse
import json
import os
import sqlite3
import sys
from functools import partial

import pika
from pika.exceptions import AMQPError

from chat_common import (EXCHANGE, HISTORY_LIMIT, HISTORY_QUEUE, clean_room,
                         make_parameters, parse_endpoint)

DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chat_history.db")


# Creation of db if it does not exists
def open_db(path):
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE IF NOT EXISTS messages (
                      seq       INTEGER PRIMARY KEY AUTOINCREMENT,
                      id        TEXT UNIQUE,
                      room      TEXT NOT NULL,
                      user      TEXT,
                      text      TEXT,
                      timestamp TEXT)""")
    db.execute("CREATE INDEX IF NOT EXISTS idx_messages_room ON messages (room, seq)")
    db.commit()
    return db

# Saving of messages
def save_message(db, message):
    db.execute("INSERT OR IGNORE INTO messages (id, room, user, text, timestamp) VALUES (?, ?, ?, ?, ?)",
               (message.get("id"), clean_room(message.get("room")), message.get("user"),
                message.get("text"), message.get("timestamp")))
    db.commit()

# Load message history
def load_history(db, room):
    rows = db.execute("SELECT id, user, room, text, timestamp FROM messages "
                      "WHERE room = ? ORDER BY seq DESC LIMIT ?", (room, HISTORY_LIMIT)).fetchall()
    rows.reverse() 
    return [{"id": r[0], "user": r[1], "room": r[2], "text": r[3], "timestamp": r[4]} for r in rows]


# RabbitMQ callbacks
def on_chat_message(db, ch, method, properties, body):
    """Every chat message passes through here, so we can save it"""
    try:
        message = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeError):
        return
    save_message(db, message)

# A joining client asks for a room's history
def on_history_request(db, ch, method, properties, body):
    try:
        room = clean_room(json.loads(body.decode("utf-8")).get("room"))
    except (ValueError, UnicodeError, AttributeError):
        return
    reply = {"room": room, "messages": load_history(db, room)}  
    if properties.reply_to:
        ch.basic_publish(exchange="", routing_key=properties.reply_to,
                         body=json.dumps(reply).encode("utf-8"))
        print(f"Sent {len(reply['messages'])} message(s) of #{room}")

# main function
def main():
    parser = argparse.ArgumentParser(description="Chat history service (SQLite)")
    parser.add_argument("endpoint", nargs="?", default="5672", help="TCP port or host:port")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite file to store messages in")
    parser.add_argument("--rabbit-user", default="guest")
    parser.add_argument("--rabbit-pass", default="guest")
    args = parser.parse_args()

    host, port = parse_endpoint(args.endpoint)
    db = open_db(args.db)
    try:
        connection = pika.BlockingConnection(make_parameters(host, port, args.rabbit_user, args.rabbit_pass))
    except AMQPError as exc:
        sys.exit(f"Could not connect to RabbitMQ at {host}:{port}. Is the Docker container running?\n{exc}")

    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=False)

    # Listen to ALL rooms
    queue = channel.queue_declare(queue="", exclusive=True, auto_delete=True).method.queue
    channel.queue_bind(exchange=EXCHANGE, queue=queue, routing_key="room.#")
    channel.basic_consume(queue=queue, on_message_callback=partial(on_chat_message, db), auto_ack=True)

    # Queue where joining clients send their history requests
    channel.queue_declare(queue=HISTORY_QUEUE)
    channel.basic_consume(queue=HISTORY_QUEUE, on_message_callback=partial(on_history_request, db), auto_ack=True)

    print(f"History service running. Saving the last {HISTORY_LIMIT} messages per room to {args.db}. Press Ctrl+C to stop...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        if connection.is_open:
            connection.close()
        db.close()


if __name__ == "__main__":
    main()