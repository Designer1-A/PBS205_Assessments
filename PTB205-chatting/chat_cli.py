"""
Command line chat
Usage: python chat_cli.py <username> <endpoint> [--room ROOM]
<endpoint> is a TCP port or host port

In-chat commands:
    /join <room> - switch to another room
    /quit  - leave
"""
import sys
import argparse
from pika.exceptions import AMQPError
from chat_common import (DEFAULT_ROOM, Listener, Publisher, clean_room, format_message, parse_endpoint)

def main():
    parser = argparse.ArgumentParser(description="PBT205 Command-Line Chat")
    parser.add_argument("username")
    parser.add_argument("endpoint", help="TCP port (5762) or host:port")
    parser.add_argument("--room", default=DEFAULT_ROOM)
    parser.add_argument("--rabbit-user", default="guest")
    parser.add_argument("--rabbit-pass", default="guest")
    args = parser.parse_args()

    host, port = parse_endpoint(args.endpoint)
    room = clean_room(args.room)

    pub = Publisher(host, port, args.rabbit_user, args.rabbit_pass)
    try:
        pub.connect()
    except AMQPError as exc:
        sys.exit(f"Could not connect to RabbitMQ at {host}:{port}, is the Docker container running?\n{exc}")

    def show_message(message):
        print(f"\r{format_message(message)}")

    def start_listener(r):
        l = Listener(host, port, r, show_message, lambda e: print(f"\n! {e}"),
                     args.rabbit_user,args.rabbit_pass)
        l.start()
        return l

    listener = start_listener(room)
    print(f"Joined room '{room}' as {args.username}. Type a message, /join <room>, or /quit.")

    try:
        while True:
            line = input()
            text = line.strip()
            if not text:
                continue
            if text == "/quit":
                break
            if text.startswith("/join"):
                parts = text.split(maxsplit=1)
                if len(parts) < 2:
                    print("Usage: /join <room>")
                    continue
                listener.stop()
                room = clean_room(parts[1])
                listener = start_listener(room)
                print(f"Joined room '{room}'.")
                continue
            try:
                pub.publish(args.username, room, text)
            except AMQPError as exc:
                print(f"Could not send: {exc}")
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        listener.stop()
        pub.close()
        print("Goodbye...")

if __name__ == "__main__":
    main()
