# A simple GUI for the contact-tracing prototype.
# It draws a grid, shows people, and lets the user search contacts.

import json
import threading
import tkinter as tk
from tkinter import messagebox
from common import connect_to_rabbitmq, set_up_exchanges, send_message, make_request_id

GRID_SIZE = 10
CELL_SIZE = 35


class ContactTracingWindow:
    def __init__(self, root):
        self.root = root
        self.root.title("PBT205 Contact-Tracing System")

        self.positions = {}
        self.request_id = None

        # Make the title and grid.
        tk.Label(
            root, text="Contact-Tracing System", font=("Arial", 14, "bold")
        ).pack(pady=5)

        self.canvas = tk.Canvas(
            root, width=GRID_SIZE * CELL_SIZE, height=GRID_SIZE * CELL_SIZE
        )
        self.canvas.pack(padx=10, pady=5)
        self.draw_grid()

        # Add the search box and button.
        search_frame = tk.Frame(root)
        search_frame.pack(pady=5)
        tk.Label(search_frame, text="Person ID:").pack(side="left")
        self.person_entry = tk.Entry(search_frame, width=15)
        self.person_entry.pack(side="left", padx=5)
        tk.Button(
            search_frame, text="Find contacts", command=self.search_contacts
        ).pack(side="left")

        self.results = tk.Text(root, width=48, height=8, state="disabled")
        self.results.pack(padx=10, pady=5)
        self.status = tk.Label(root, text="Connecting to RabbitMQ...")
        self.status.pack(pady=5)

        # RabbitMQ work runs in a separate thread so the window can stay responsive.
        self.connection = None
        self.channel = None
        self.response_queue = None
        threading.Thread(target=self.listen_for_messages, daemon=True).start()

        self.root.protocol("WM_DELETE_WINDOW", self.close_window)

    def draw_grid(self):
        # Draw the lines for the 10 by 10 board.
        for number in range(GRID_SIZE + 1):
            point = number * CELL_SIZE
            self.canvas.create_line(
                point, 0, point, GRID_SIZE * CELL_SIZE, fill="gray"
            )
            self.canvas.create_line(
                0, point, GRID_SIZE * CELL_SIZE, point, fill="gray"
            )

    def listen_for_messages(self):
        try:
            self.connection = connect_to_rabbitmq()
            self.channel = self.connection.channel()
            set_up_exchanges(self.channel)

            # The GUI gets its own queue for position updates.
            self.channel.queue_declare(queue="gui.positions")
            self.channel.queue_bind(
                queue="gui.positions",
                exchange="position",
                routing_key="position.#"
            )

            # Use a temporary queue for contact query responses.
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.response_queue = result.method.queue
            self.channel.queue_bind(
                queue=self.response_queue,
                exchange="query-response",
                routing_key="query.response"
            )

            self.channel.basic_consume(
                queue="gui.positions",
                on_message_callback=self.receive_position
            )
            self.channel.basic_consume(
                queue=self.response_queue,
                on_message_callback=self.receive_query_response
            )
            self.root.after(0, lambda: self.status.config(text="Connected to RabbitMQ."))
            self.channel.start_consuming()
        except Exception as error:
            self.root.after(
                0, lambda: self.status.config(text=f"Connection error: {error}")
            )

    def receive_position(self, channel, method, properties, body):
        try:
            position = json.loads(body.decode("utf-8"))
            person_id = str(position["person_id"])
            x = int(position["x"])
            y = int(position["y"])
            self.positions[person_id] = (x, y)
            self.root.after(0, self.draw_people)
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            pass
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def draw_people(self):
        # Remove the old dots and draw each person's latest position.
        self.canvas.delete("person")
        for person_id, position in self.positions.items():
            x, y = position
            if 0 <= x < GRID_SIZE and 0 <= y < GRID_SIZE:
                centre_x = x * CELL_SIZE + CELL_SIZE // 2
                centre_y = y * CELL_SIZE + CELL_SIZE // 2
                self.canvas.create_oval(
                    centre_x - 7, centre_y - 7,
                    centre_x + 7, centre_y + 7,
                    fill="blue", tags="person"
                )
                self.canvas.create_text(
                    centre_x, centre_y + 12,
                    text=person_id[:6], font=("Arial", 7), tags="person"
                )

    def search_contacts(self):
        # Send a query to the tracker when the user clicks the button.
        person_id = self.person_entry.get().strip()
        if not person_id:
            messagebox.showwarning("Missing ID", "Please enter a person ID.")
            return

        if not self.channel or not self.response_queue:
            messagebox.showerror("Not connected", "RabbitMQ is not connected yet.")
            return

        self.request_id = make_request_id()
        query = {"person_id": person_id, "request_id": self.request_id}
        send_message(self.channel, "query", "query.request", query)
        self.status.config(text=f"Query sent for {person_id}.")

    def receive_query_response(self, channel, method, properties, body):
        try:
            response = json.loads(body.decode("utf-8"))
            # Ignore responses that belong to an earlier query.
            if response.get("request_id") == self.request_id:
                lines = [f"Contacts for {response['person_id']} (newest first):"]
                contacts = response.get("contacts", [])
                if not contacts:
                    lines.append("No contacts recorded.")
                for contact in contacts:
                    lines.append(
                        f"- {contact['person_id']} at "
                        f"({contact['x']}, {contact['y']})"
                    )
                output = "\n".join(lines)
                self.root.after(0, lambda: self.show_results(output))
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            pass
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def show_results(self, output):
        self.results.config(state="normal")
        self.results.delete("1.0", "end")
        self.results.insert("end", output)
        self.results.config(state="disabled")
        self.status.config(text="Query response received.")

    def close_window(self):
        # Close RabbitMQ when the user closes the window.
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass
        self.root.destroy()


if __name__ == "__main__":
    window = tk.Tk()
    app = ContactTracingWindow(window)
    window.mainloop()
