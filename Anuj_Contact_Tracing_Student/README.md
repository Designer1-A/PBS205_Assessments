# PBT205 Contact-Tracing Prototype
## Anuj's part — student version

This folder contains a simple Python prototype for the contact-tracing part of the group project.

### Files
- `person.py` — makes a person and sends their position to RabbitMQ.
- `tracker.py` — receives positions, keeps the latest position for each person, records contacts, and answers queries.
- `query.py` — asks the tracker for one person's contact history.
- `gui.py` — simple Tkinter window showing a grid and allowing a contact query.
- `common.py` — shared RabbitMQ setup and message helper functions.
- `requirements.txt` — Python package needed for RabbitMQ.

### 1. Requirements
- Python 3
- Docker Desktop (or a RabbitMQ server already running)
- Internet access the first time Docker downloads the RabbitMQ image

Install the Python package from a terminal in this folder:
```bash
pip install -r requirements.txt
```

### 2. Start RabbitMQ
If RabbitMQ is not already running, use:
```bash
docker run -d --name pbt205-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```
If the container already exists, start it with:
```bash
docker start pbt205-rabbitmq
```
RabbitMQ's management page is at `http://localhost:15672`. The default local login is usually `guest` / `guest`.

### 3. Run the tracker first
Open a terminal in this folder:
```bash
python tracker.py
```
Keep this terminal open.

### 4. Run two people
Open two more terminals in the same folder:
```bash
python person.py Alice
```
```bash
python person.py Bob
```
Each person starts at a random position and moves every second. You can use different names.

Optional settings:
```bash
python person.py Alice --size 10 --speed 1
```
`--size` sets the square grid size. `--speed` is the number of seconds between moves.

### 5. Search for contacts
In another terminal:
```bash
python query.py Alice
```
The result should list contacts newest first. It may say no contacts if Alice and Bob have not occupied the same square while the tracker was processing their updates. Let them run longer and query again.

### 6. Open the GUI
With the tracker and people running, open another terminal:
```bash
python gui.py
```
Enter a person ID (for example `Alice`) and click **Find contacts**.

### RabbitMQ message names
The assessment refers to `position`, `query`, and `query-response`. This project uses those names as RabbitMQ topic exchanges:
- `position` exchange, routing key `position.update`
- `query` exchange, routing key `query.request`
- `query-response` exchange, routing key `query.response`

### Testing notes
Run the programs yourself and take screenshots of actual results. Do not mark a test as passed until you have observed the expected result.

Suggested tests:
1. Start the tracker, then start Alice and Bob. Confirm that the tracker prints position/contact messages.
2. Query Alice and confirm the query returns a response.
3. Query someone who has no contacts and confirm it reports no contacts.
4. Use a small board, for example `--size 2`, to make it easier for people to meet.
5. Check that coordinates remain between zero and `size - 1`.

### Limitations
- Contact history is stored in memory and is cleared when the tracker stops.
- Messages arrive asynchronously, so this is a basic demonstration rather than a real public-health contact-tracing system.
- The GUI uses a 10 by 10 grid. For this simple version, run people with the default board size of 10 when using the GUI.
