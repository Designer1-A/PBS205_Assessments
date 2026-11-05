import json, re, threading, pika
from datetime import datetime, timezone
from pika.exceptions import AMQPError

EXCHANGE = "chat"
DEFAULT_ROOM = "United"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5672

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
    return json.dumps({"user": user, "room": clean_room(room), "text": text, "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),})

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
    def __init__(self, host, port, room, on_message, on_error=None, rabbit_user="guest", rabbit_pass="guest"):
        super().__init__(daemon=True)
        self._params = make_parameters(host, port, rabbit_user, rabbit_pass)
        self.room = clean_room(room)
        self._on_message = on_message
        self._on_error = on_error or (lambda e: None)
        self._conn = None
        self._channel = None
        self._stopping = False

    def run(self):
        try:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=False)

            queue = self._channel.queue_declare(queue="", exclusive=True, auto_delete=True).method.queue
            self._channel.queue_bind(exchange=EXCHANGE, queue=queue, routing_key=routing_key(self.room))
            self._channel.basic_consume(queue=queue, on_message_callback=self._callback, auto_ack=True)
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

    def _callback(self, ch, method, properties, body):
        try:
            self._on_message(json.loads(body.decode("utf-8")))
        except (ValueError, UnicodeError):
            pass

    def stop(self):
        self._stopping = True
        if self._conn and self._conn.is_open:
            try:
                self._conn.add_callback_threadsafe(self._channel.stop_consuming)
            except AMQPError:
                pass
        self.join(timeout=3)


            