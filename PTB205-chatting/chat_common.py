import json, re, threading, pika, uuid
from datetime import datetime, timezone
from pika.exceptions import AMQPError, ChannelClosedByBroker

EXCHANGE = "chat" # topic exchange used by all rooms
DEFAULT_ROOM = "United"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5672
HISTORY_QUEUE = "chat.history.requests" # a queue where clients ask for message history
HISTORY_LIMIT = 10 # how many recent messages we keep per room
HISTORY_WAIT_SECONDS = 2 # how long a new joiner waits for history

def parse_endpoint(endpoint):
    endpoint = (endpoint or "").strip()
    if not endpoint:
        return DEFAULT_HOST, DEFAULT_PORT
    if endpoint.isdigit():
        return DEFAULT_HOST, int(endpoint)
    if ":" in endpoint:
        host, port = endpoint.rsplit(":", 1)
        return host or DEFAULT_HOST, int(port)
    return endpoint, DEFAULT_PORT

def clean_room(room):
    room = re.sub(r"[^a-z0-9_-]+", "_", (room or DEFAULT_ROOM).strip().lower())
    return room.strip("_") or DEFAULT_ROOM

def routing_key(room):
    return f"room.{clean_room(room)}"

def make_parameters(host, port, rabbit_user="guest", rabbit_pass="guest", heartbeat=60):
    return pika.ConnectionParameters(host = host, port = port, credentials = pika.PlainCredentials(rabbit_user, rabbit_pass), heartbeat = heartbeat, connection_attempts=1, socket_timeout=5)

def build_message(user, room, text):
    return json.dumps({"id": uuid.uuid4().hex, "user": user, "room": clean_room(room), "text": text, "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),})

def format_message(message):
    try:
        ts = datetime.fromisoformat(message["timestamp"]).astimezone().strftime("%H:%M:%S")
    except (KeyError, ValueError):
        ts = "--:--"
    return f"[{ts}] {message.get('user', '?')}: {message.get('text', '')}"

class Publisher:
    def __init__(self, host, port, rabbit_user="guest", rabbit_pass="guest"):
        self._params = make_parameters(host, port, rabbit_user, rabbit_pass, heartbeat=0)
        self._conn = None
        self._channel = None
        self._lock = threading.Lock()

    def connect(self):
        self._conn = pika.BlockingConnection(self._params)
        self._channel = self._conn.channel()
        self._channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=False)

    def publish(self, user, room, text):
        body = build_message(user, room, text)
        with self._lock:
            for attempt in (1, 2):
                try:
                    if self._channel is None or self._channel.is_closed:
                        self.connect()
                    self._channel.basic_publish(exchange=EXCHANGE, routing_key=routing_key(room), body=body.encode("utf-8"))
                    return
                except AMQPError:
                    self._channel = None
                    if attempt == 2:
                        raise

    def close(self):
        try:
            if self._conn and self._conn.is_open:
                self._conn.close()
        except AMQPError:
            pass

class Listener(threading.Thread):
    def __init__(self, host, port, room, on_message, on_error=None, rabbit_user="guest", rabbit_pass="guest",on_notice=None):
        super().__init__(daemon=True)
        self._params = make_parameters(host, port, rabbit_user, rabbit_pass)
        self.room = clean_room(room)
        self._on_message = on_message
        self._on_error = on_error or (lambda e: None)
        self._on_notice = on_notice or (lambda t: None)
        self._conn = None
        self._channel = None
        self._stopping = False
        self._loading = True # Remains true until the history has arrived
        self._buffer = [] # live messages that arrive while we wait for the history

    def run(self):
        try:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=False)
 
            # Subscribe to the room FIRST, so no live message can be missed
            live_q = self._channel.queue_declare(queue="", exclusive=True, auto_delete=True).method.queue
            self._channel.queue_bind(exchange=EXCHANGE, queue=live_q, routing_key=routing_key(self.room))
            self._channel.basic_consume(queue=live_q, on_message_callback=self._on_live, auto_ack=True)
 
            # Then ask the history service for the latest messages of this room
            reply_q = self._channel.queue_declare(queue="", exclusive=True, auto_delete=True).method.queue
            self._channel.basic_consume(queue=reply_q, on_message_callback=self._on_history, auto_ack=True)
            self._channel.basic_publish(exchange="", routing_key=HISTORY_QUEUE,
                                        properties=pika.BasicProperties(reply_to=reply_q),
                                        body=json.dumps({"room": self.room}).encode("utf-8"))
 
            # If the history service never answers, carry on without history
            self._conn.call_later(HISTORY_WAIT_SECONDS, self._history_timeout)
            self._channel.start_consuming()
        except AMQPError as exc:
            if not self._stopping:
                self._on_error(f"Connection to RabbitMQ lost: {exc}")
        finally:
            try:
                if self._conn and self._conn.is_open:
                    self._conn.close()
            except AMQPError:
                pass

    # live messages
    def _on_live(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeError):
            return                              
        if self._loading:
            self._buffer.append(message)         
        else:
            self._on_message(message)

    def _on_history(self, ch, method, properties, body):
        if not self._loading:
            return
        try:
            history = json.loads(body.decode("utf-8")).get("messages", [])
        except (ValueError, UnicodeError, AttributeError):
            history = []
        self._finish_loading(history)
 
    def _history_timeout(self):
        if self._loading:
            self._on_notice("history service not reachable, showing live messages only")
            self._finish_loading([])
 
    def _finish_loading(self, history):
        """Show the history (oldest first), then the live messages that arrived meanwhile"""
        self._loading = False
        if history:
            self._on_notice("-- earlier messages --")
            for message in history:
                self._on_message(message)
            self._on_notice("-- end of history --")
        shown = {m.get("id") for m in history}
        for message in self._buffer:
            if message.get("id") not in shown:
                self._on_message(message)
        self._buffer = []

    def stop(self):
        self._stopping = True
        if self._conn and self._conn.is_open:
            try:
                self._conn.add_callback_threadsafe(self._channel.stop_consuming)
            except AMQPError:
                pass
        self.join(timeout=3)


            