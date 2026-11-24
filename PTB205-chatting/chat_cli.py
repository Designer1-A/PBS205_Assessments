"""
Command line chat
Usage: python chat_cli.py <username> <endpoint> [--room ROOM]
<endpoint> is a TCP port or host port

In-chat commands:
    /join <room> - switch to another room
    /quit  - leave
"""
import argparse
import sys

from pika.exceptions import AMQPError

from chat_common import (DEFAULT_ROOM, Listener, Publisher, UsernameTaken, clean_room,
                         format_message, parse_endpoint)


def main():
    parser = argparse.ArgumentParser(description="PBT205 Command-Line Chat")
    parser.add_argument("username")
    parser.add_argument("endpoint", help="TCP port (5672) or host:port")
    parser.add_argument("--room", default=DEFAULT_ROOM)
    parser.add_argument("--rabbit-user", default="guest")
    parser.add_argument("--rabbit-pass", default="guest")
    args = parser.parse_args()

    host, port = parse_endpoint(args.endpoint)
    room = clean_room(args.room)

    # Passing the username makes the Publisher claim it. If it is taken, connect() fails
    pub = Publisher(host, port, args.rabbit_user, args.rabbit_pass, username=args.username)
    try:
        pub.connect()
    except UsernameTaken:
        sys.exit(f"The username '{args.username}' is already in use. Please choose a different name.")
    except AMQPError as exc:
        sys.exit(f"Could not connect to RabbitMQ at {host}:{port}, is the Docker container running?\n{exc}")

    def show_message(message):
        print(f"\r{format_message(message)}")

    def show_notice(text):
        print(f"\r{text}")

    def start_listener(r):
        # The listener now also fetches the room's history before showing live messages
        l = Listener(host, port, r, show_message, lambda e: print(f"\n! {e}"),
                     on_notice=show_notice, rabbit_user=args.rabbit_user, rabbit_pass=args.rabbit_pass)
        l.start()
        return l

    listener = start_listener(room)
    print(f"Joined room '{room}' as {args.username}. Type a message, /join <room>, or /quit.")

    try:
        while True:
            text = input().strip()
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
            except UsernameTaken:
                print("! Your username was taken while you were disconnected. Please restart the chat...")
                break
            except AMQPError as exc:
                print(f"Could not send: {exc}")
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        listener.stop()
        pub.close()                       
        print("Goodbye... See you soon...")


if __name__ == "__main__":
    main()