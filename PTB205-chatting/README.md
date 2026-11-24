# Chat prototype, Assessment 3(message history and unique usernames)

## Run order
1. RabbitMQ:      docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
2. Install:       pip install -r requirements.txt   (same as before: pika)
3. History:       python service_history.py 5672    (start ONE, before the chat clients)
4. Chat clients:  python chat_cli.py <username> 5672 --room <room>
                  python chat_gui.py

## What changed
- Message history: a user who joins a room sees its last 10 messages first, oldest to newest.
- Unique usernames: a name already in use (any letter case) is refused. The name is free again when its user leaves.
- Messages now include an "id" field (user, room, text, timestamp are unchanged).

## Notes
- History is saved in a SQLite file (chat_history.db), so it survives a restart of history_service.py. Messages sent while the service is switched off are not saved.
- Without the history service the chat still works, but shows live messages only.