import queue
import tkinter as tk
from tkinter import messagebox, ttk
from pika.exceptions import AMQPError
from chat_common import (DEFAULT_ROOM, Listener, Publisher, clean_room, format_message, parse_endpoint)

PRESET_ROOMS = ["United", "Como", "Milan", "Bologna", "Random", "Tech", "Study"]

class ChatApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Ahmed's PBT205 Chat")
        self.geometry("800x500")
        self.incoming = queue.Queue()
        self.pub = self.listener = None
        self.username = self.room = ""
        self.host, self.port = "localhost", 5672
        self._build_login()
        self.after(100, self._drain)

    # login screen
    def _build_login(self):
        self.login = ttk.Frame(self, padding=24)
        self.login.pack(fill="both", expand=True)
        ttk.Label(self.login, text="Please join a chat room", font=("", 16, "bold")).grid(columnspan=2, pady=(0, 14))
        self.v_user = tk.StringVar()
        self.v_endpoint = tk.StringVar(value="localhost:5672")
        self.v_room = tk.StringVar(value=DEFAULT_ROOM)
        for i, (label, var) in enumerate([("Username", self.v_user), ("Endpoint", self.v_endpoint)], start=1):
            ttk.Label(self.login, text=label).grid(row=i, column=0, sticky="w", pady=4)
            ttk.Entry(self.login, textvariable=var, width=28).grid(row=i, column=1, pady=4)
        ttk.Label(self.login, text="Room").grid(row=3, column=0, sticky="w", pady=4)
        ttk.Combobox(self.login, textvariable=self.v_room, values=PRESET_ROOMS, width=26).grid(row=3, column=1, pady=4)
        ttk.Button(self.login, text="Join", command=self._join).grid(row=4, columnspan=2, pady=14)
        self.bind("<Return>", lambda e:self._join() if self.login.winfo_exists() and self.login.winfo_ismapped() else None)

    def _join(self):
        user = self.v_user.get().strip()
        if not user:
            messagebox.showwarning("Username is needed, Please enter a Username: ")
            return
        try:
            self.host, self.port = parse_endpoint(self.v_endpoint.get())
        except ValueError:
            messagebox.showerror("Bad endpoint, use port (5672) or host:port")
            return
        pub = Publisher(self.host, self.port)
        try:
            pub.connect()
        except AMQPError as exc:
            messagebox.showerror("Conection failed",
                                 f"Could not reach RabbitMQ at {self.host}:{self.port}.\n"
                                 f"Is the Docker container running?\n\n{exc}")
            return
        self.pub, self.username = pub, user

        self.unbind("<Return>")
        self.login.destroy()
        self._build_chat()
        self._enter_room(self.v_room.get())

    def _build_chat(self):
        self.chat = ttk.Frame(self, padding=10)
        self.chat.pack(fill="both", expand=True)

        top = ttk.Frame(self.chat)
        top.pack(fill="x")
        self.label_room = ttk.Label(top, font=("", 12, "bold"))
        self.label_room.pack(side="left")
        ttk.Button(top, text="Switch", command=self._switch).pack(side="right")
        self.v_switch = tk.StringVar()
        ttk.Combobox(top, textvariable=self.v_switch, values=PRESET_ROOMS, width=14).pack(side="right", padx=6)

        body = ttk.Frame(self.chat)
        body.pack(fill="both", expand=True, pady=8)
        self.log = tk.Text(body, state="disabled", wrap="word")
        self.log.tag_configure("me", foreground="#0b6e4f")
        self.log.tag_configure("sys", foreground="#888888")
        sb = ttk.Scrollbar(body, command=self.log.yview)
        self.log.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.log.pack(side="left", fill="both", expand=True)

        bottom = ttk.Frame(self.chat)
        bottom.pack(fill="x")
        self.v_msg = tk.StringVar()
        entry = ttk.Entry(bottom, textvariable=self.v_msg)
        entry.pack(side="left", fill="x", expand=True)
        entry.bind("<Return>", lambda e: self._send())
        entry.focus_set()
        ttk.Button(bottom, text="Send", command=self._send).pack(side="right", padx=(6, 0))
        self.protocol("WM_DELETE_WINDOW", self._close)

    def _enter_room(self, room):
        if self.listener:
            self.listener.stop()
        self.room = clean_room(room)
        self.listener = Listener(self.host, self.port, self.room,
                                 lambda m: self.incoming.put(("msg", m)),
                                 lambda e: self.incoming.put(("err", e)))
        self.listener.start()
        self.label_room.config(text=f"#{self.room}   ({self.username})")
        self._append(f"-- joined #{self.room} --", "sys")

    def _switch(self):
        new = self.v_switch.get().strip()
        if new and clean_room(new) != self.room:
            self._enter_room(new)

    def _send(self):
        text = self.v_msg.get().strip()
        if not text:
            return
        try:
            self.pub.publish(self.username, self.room, text)
            self.v_msg.set("")
        except AMQPError as exc:
            self._append(f"! could not send: {exc}", "sys")

    def _drain(self):
        try:
            while True:
                kind, payload = self.incoming.get_nowait()
                if kind == "msg":
                    mine = payload.get("user") == self.username
                    self._append(format_message(payload), "me" if mine else None)
                else:
                    self._append(f"! {payload}", "sys")
        except queue.Empty:
            pass
        self.after(100, self._drain)

    def _append(self, line, tag=None):
        self.log.configure(state="normal")
        self.log.insert("end", line + "\n", tag)
        self.log.configure(state="disabled")
        self.log.see("end")

    def _close(self):
        if self.listener:
            self.listener.stop()
        if self.pub:
            self.pub.close()
        self.destroy()


if __name__ == "__main__":
    ChatApp().mainloop()
