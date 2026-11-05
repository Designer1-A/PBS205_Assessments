# This program represents one person moving around a grid.
# It sends the person's current position to RabbitMQ.

import argparse
import random
import time
from common import connect_to_rabbitmq, set_up_exchanges, send_message


def main():
    # Read the person's name and optional settings from the terminal.
    parser = argparse.ArgumentParser(description="Send a person's position.")
    parser.add_argument("person_id", help="A unique name for this person")
    parser.add_argument("--size", type=int, default=10, help="Grid size")
    parser.add_argument("--speed", type=float, default=1.0, help="Seconds between moves")
    args = parser.parse_args()

    # Check that the settings are sensible.
    if args.size < 1:
        parser.error("Grid size must be at least 1")
    if args.speed <= 0:
        parser.error("Speed must be greater than 0")

    # Choose a random starting square.
    x = random.randint(0, args.size - 1)
    y = random.randint(0, args.size - 1)

    connection = connect_to_rabbitmq()
    channel = connection.channel()
    set_up_exchanges(channel)

    print(f"{args.person_id} starts at ({x}, {y})")

    try:
        while True:
            # Send the current position to the tracker.
            position = {
                "person_id": args.person_id,
                "x": x,
                "y": y,
                "board_size": args.size,
                "timestamp": time.time()
            }
            send_message(channel, "position", "position.update", position)
            print(f"Sent position: {args.person_id} at ({x}, {y})")

            # Wait before moving to the next square.
            time.sleep(args.speed)

            # Choose a move to a neighbouring square, or stay still.
            move_x, move_y = random.choice([
                (-1, -1), (-1, 0), (-1, 1),
                (0, -1), (0, 0), (0, 1),
                (1, -1), (1, 0), (1, 1)
            ])

            # Keep the person inside the grid.
            x = max(0, min(args.size - 1, x + move_x))
            y = max(0, min(args.size - 1, y + move_y))

    except KeyboardInterrupt:
        print(f"Stopped {args.person_id}.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
