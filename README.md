# tibems-python

A Python wrapper for the **TIBCO Enterprise Message Service (EMS)** C API. It uses `ctypes` to call the native `libtibems.so` shared library, exposing Pythonic context managers for connections, sessions, messages, producers, and consumers.

## Author

**Vadym Korol** — [vadym.korol@verbundo.com](mailto:vadym.korol@verbundo.com)

## License

This project is licensed under the [MIT License](LICENSE).

## Prerequisites

Place the TIBCO EMS shared libraries in `tibems/lib/`:
- `libtibems.so` (required)
- `libssl.so.3`, `libcrypto.so.3`, `libz.so.1.2.13`, `libjemalloc.so.2` (dependencies)

Set `LD_LIBRARY_PATH` at runtime:
```bash
export LD_LIBRARY_PATH=$PWD/tibems/lib:$LD_LIBRARY_PATH
```

## Configuration

Set environment variables (via `.env` file or shell exports):
```
TIBEMS_URL=ssl://host:7243    # or tcp://host:7222
TIBEMS_USER=username
TIBEMS_PASSWORD=password
```

Place PEM certificates in `certs/` for SSL connections.

## Installation

No pip install needed — the library is used in-place. Import directly:

```python
from tibems import (
    JmsPropertyType, JMS_Property, AckMode, DestinationType,
    tibems_connection, tibems_session, tibems_message,
    create_destination, create_producer, create_consumer,
    publish_message, ReceivedMessage, ReplyTo,
)
```

---

## Usage

### Publish to a Queue

```python
# import required classes and functions
from tibems import (
    JmsPropertyType, JMS_Property, AckMode, DestinationType,
    tibems_connection, tibems_session, tibems_message,
    create_destination, create_producer, publish_message,
)

# 1. create a connection
# use 'start_connection=True' if you want to consume a messages from the connection (i.e. get a reply to your request)
# use 'server_cert' if url starts with 'ssl'
# use 'verify_server_cert=False' to avoid checking SSL certificates, in that case you don't need 'server_cert'
with tibems_connection(
    url="ssl://host:7243",
    username="user",
    password="pass",
    start_connection=True,
    server_cert="certs/root_CA.pem",
    verify_server_cert=True
) as connection:
    # 2. create a session specifying whether it should be transacted and specifying an aknowledgement mode
    with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
        # 3. create a destination and a producer
        queue = create_destination(name="my.queue", type=DestinationType.Queue)
        producer = create_producer(session, queue)

        # 4. create a message:
        # specify message body, and if needed, JMS properties
        with tibems_message(
            message_text="Hello, Queue!",
            jms_props=[
                JMS_Property(name="app.name", value="my-app", type=JmsPropertyType.String),
                JMS_Property(name="retry.count", value=0, type=JmsPropertyType.Integer),
            ]
        ) as message:
            # 5. publish message to a destination
            publish_message(producer, message=message)
```

### Publish with a Reply (Request/Reply Pattern)

Send a message and wait for a reply using a temporary queue:

```python

with tibems_connection(...) as connection:
    with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
        queue = create_destination(name="request.queue", type=DestinationType.Queue)
        producer = create_producer(session, queue)

        with tibems_message(message_text="Ping") as message:
            # use 'expect_reply=True' to specify, that a reply is expected
            # do not provide 'reply_destination' parameter
            reply = publish_message(
                producer,
                message=message,
                expect_reply=True,
                session=session,
                reply_timeout=5000  # wait up to 5 seconds (ms); omit for blocking wait
            )
            if reply:
                print(f"Got reply: {reply.body}")
                for prop in reply.properties:
                    print(f"  {prop['name']}: {prop['value']}")
            else:
                print("No reply received within timeout")
```

Send a message and wait for a reply using a specific reply queue:

```python

with tibems_connection(...) as connection:
    with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
        queue = create_destination(name="request.queue", type=DestinationType.Queue)
        producer = create_producer(session, queue)

        with tibems_message(message_text="Ping") as message:
            # use 'expect_reply=True' to specify, that a reply is expected
            # use 'reply_destination' parameter to secify a name of reply queue
            reply = publish_message(
                producer,
                message=message,
                expect_reply=True,
                reply_destination='response.queue',
                session=session,
                reply_timeout=5000  # wait up to 5 seconds (ms); omit for blocking wait
            )
            if reply:
                print(f"Got reply: {reply.body}")
                for prop in reply.properties:
                    print(f"  {prop['name']}: {prop['value']}")
            else:
                print("No reply received within timeout")
```

### Publish to a Topic

```python
with tibems_connection(...) as connection:
    with tibems_session(connection=connection) as session:
        topic = create_destination(name="my.topic", type=DestinationType.Topic)
        producer = create_producer(session, topic)

        with tibems_message(message_text="Hello, Topic!") as message:
            publish_message(producer, message=message)
```

### Consume Messages

```python
import signal
from tibems import (
    AckMode, DestinationType,
    tibems_connection, tibems_session, tibems_message,
    create_destination, create_consumer, create_producer, publish_message,
)

with tibems_connection(...) as connection:
    with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
        queue = create_destination(name="my.queue", type=DestinationType.Queue)

        with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:
            # Graceful shutdown on Ctrl+C
            signal.signal(signal.SIGINT, lambda *_: consumer.stop())

            for msg in consumer:
                print(f"Received: {msg.body}")
                print("JMS Properties:")
                for prop in msg.properties:
                    print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

                # Request/Reply: respond to the sender
                if msg.reply_to:
                    producer = create_producer(session, msg.reply_to.handle)
                    with tibems_message(
                        message_text="Reply",
                        jms_props=[],
                        correlation_id=msg.message_id
                    ) as reply:
                        publish_message(producer, reply)

                # msg.acknowledge()  # only needed when using CLIENT_ACK mode
```

### Consume from a Topic

```python
import signal
from tibems import (
    AckMode, DestinationType,
    tibems_connection, tibems_session,
    create_destination, create_consumer,
)

with tibems_connection(..., start_connection=True) as connection:
    with tibems_session(connection=connection) as session:
        topic = create_destination(name="my.topic", type=DestinationType.Topic)

        with create_consumer(session, topic, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:
            signal.signal(signal.SIGINT, lambda *_: consumer.stop())

            for msg in consumer:
                print(f"Received from topic: {msg.body}")
```

### Filter Messages with a Selector

Pass a JMS selector (SQL-92 syntax) to `create_consumer` to receive only matching messages. Non-matching messages remain on the destination for other consumers.

```python
with tibems_connection(..., start_connection=True) as connection:
    with tibems_session(connection=connection) as session:
        queue = create_destination(name="my.queue")

        selector = "region = 'EU' AND priority > 3"
        with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_AUTO_ACK, selector=selector) as consumer:
            signal.signal(signal.SIGINT, lambda *_: consumer.stop())

            for msg in consumer:
                print(f"Received: {msg.body}")
```

### Consume Messages with CLIENT_ACK

When using `CLIENT_ACK`, messages are **not** automatically acknowledged. You must call `msg.acknowledge()` to confirm processing — otherwise the broker may redeliver them:

```python
with tibems_connection(...) as connection:
    with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_CLIENT_ACK) as session:
        queue = create_destination(name="my.queue", type=DestinationType.Queue)

        with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_CLIENT_ACK) as consumer:
            signal.signal(signal.SIGINT, lambda *_: consumer.stop())

            for msg in consumer:
                try:
                    print(f"Received: {msg.body}")
                    # Process the message...
                    process(msg.body)

                    # Acknowledge only after successful processing
                    msg.acknowledge()
                except Exception:
                    # Skip acknowledge on failure — message will be redelivered
                    print(f"Failed to process message: {msg.body}")
```

> **Note:** In `CLIENT_ACK` mode, acknowledging one message also acknowledges **all** previously received messages on that session. If you need per-message acknowledgment, use a dedicated session per message or switch to `AUTO_ACK`.

---

## API Reference

### Context Managers

| Function | Description |
|---|---|
| `tibems_connection(url, username, password, start_connection=False, server_cert=None, verify_server_cert=True)` | Opens and closes an EMS connection. Handles SSL when URL starts with `ssl:`. |
| `tibems_session(connection, transacted, ack_mode)` | Creates and closes an EMS session. |
| `tibems_message(message_text, jms_props=[], correlation_id=None)` | Creates and destroys a text message with optional JMS properties. |
| `create_consumer(session, destination, ack_mode, selector=None, no_local=False)` | Returns a `TibEMSConsumer` that is iterable (`for msg in consumer`). |

### Core Functions

| Function | Description |
|---|---|
| `create_destination(name, type)` | Create a `Queue` or `Topic` destination. |
| `create_producer(session, destination)` | Create a message producer for the given destination. |
| `publish_message(producer, message, expect_reply=False, reply_destination=None, session=None, reply_timeout=None)` | Send a message. Optionally waits for a reply on a temporary queue. |

### Enums

| Enum | Values |
|---|---|
| `AckMode` | `TIBEMS_AUTO_ACK`, `TIBEMS_CLIENT_ACK`, `TIBEMS_DUPS_OK_ACK` |
| `DestinationType` | `Queue`, `Topic` |
| `JmsPropertyType` | `String`, `Boolean`, `Byte`, `Short`, `Integer`, `Long`, `Float`, `Double` |

### ReceivedMessage

Each message yielded from the consumer has:

| Attribute | Description |
|---|---|
| `body` | Text content of the message. |
| `properties` | List of dicts: `{"name": str, "type": str, "value": any}`. Includes custom and standard JMS headers (`JMSMessageID`, `JMSCorrelationID`, `JMSReplyTo`, etc.). |
| `reply_to` | `ReplyTo` object with a `handle` usable as a producer destination, or `None`. |
| `message_id` | The `JMSMessageID` string, or `None`. |
| `acknowledge()` | Call to ack the message (required only in `CLIENT_ACK` mode). |

### TibEMSConsumer

| Method | Description |
|---|---|
| `stop()` | Signal the consumer to stop after the current poll returns. Use with `signal.signal(signal.SIGINT, ...)` for graceful shutdown. |

---

## One-Shot Convenience Functions

For quick fire-and-forget publishing without managing context manually:

```python
from tibems import queue_publish, topic_publish, JmsPropertyType, JMS_Property

queue_publish(
    ems_url="tcp://host:7222",
    ems_username="user",
    ems_password="pass",
    queue_name="my.queue",
    message_text="Hello",
    jms_props=[JMS_Property(name="key", value="val", type=JmsPropertyType.String)]
)

topic_publish(
    ems_url="tcp://host:7222",
    ems_username="user",
    ems_password="pass",
    topic_name="my.topic",
    message_text="Hello",
    jms_props=[]
)
```

## SSL / Certificates

For SSL connections (`ssl://` URL):
- `server_cert` — path to the CA certificate for server verification
- `client_cert` — path to client identity in `.p12` format (optional, for mutual TLS)
- Set `verify_server_cert=False` to skip host verification (disables both `SetVerifyHost` and `SetVerifyHostName`)

---

## Examples

Runnable scripts in the [`examples/`](examples/) directory:

| File | Description |
|---|---|
| [`send_to_queue_no_reply_no_ssl.py`](examples/send_to_queue_no_reply_no_ssl.py) | Publish to a queue over TCP, no reply expected |
| [`send_to_queue_no_reply_use_ssl.py`](examples/send_to_queue_no_reply_use_ssl.py) | Publish to a queue over SSL, no reply expected |
| [`send_to_queue_await_reply.py`](examples/send_to_queue_await_reply.py) | Publish to a queue and block for a reply (request/reply producer side) |
| [`publish_to_topic.py`](examples/publish_to_topic.py) | Publish to a topic over TCP |
| [`receive_from_queue.py`](examples/receive_from_queue.py) | Consume messages from a queue |
| [`receive_from_topic.py`](examples/receive_from_topic.py) | Subscribe to a topic and consume messages |
| [`receive_from_queue_send_reply.py`](examples/receive_from_queue_send_reply.py) | Consume messages and send a correlated reply to `JMSReplyTo` (request/reply responder side) |
| [`receive_from_queue_client_ack.py`](examples/receive_from_queue_client_ack.py) | Consume with `CLIENT_ACK` — manually acknowledge after processing |
| [`receive_from_queue_with_selector.py`](examples/receive_from_queue_with_selector.py) | Consume with a JMS selector to filter messages by property values |
