# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Demonstrates robust consumption from a queue with automatic reconnection.
#
# When the EMS server becomes unreachable the exception_listener fires,
# signals the consumer to stop, and the outer retry loop recreates the
# entire connection / session / consumer stack after a short delay.
#
# In a fault-tolerant (FT) EMS setup the broker handles reconnection
# internally; the exception_listener still fires with status updates so
# you can log or react to them without any manual reconnect logic.

import sys
import os
import signal
import threading
import time

# add parent directory to sys.path to allow imports from the root of the project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    AckMode,
    tibems_connection,
    tibems_session,
    create_destination,
    create_consumer,
)

QUEUE_NAME = "my.queue"
MAX_RETRIES = 10
RECONNECT_DELAY_S = 5


def consume(shutdown: threading.Event) -> bool:
    """Open one connection and drain the queue until stopped or disconnected.

    Returns True when the caller requested a clean shutdown (Ctrl+C),
    False when a connection event interrupted the session (trigger reconnect).
    """
    connection_lost = threading.Event()

    def on_exception(event_text: str) -> None:
        # Called on an EMS internal thread — keep it short and thread-safe.
        print(f"[EMS] Connection event: {event_text}")
        connection_lost.set()

    try:
        with tibems_connection(
            url='tcp://localhost:7222',
            username='test_user',
            password='test_password',
            start_connection=True,
            exception_listener=on_exception,
        ) as connection:

            with tibems_session(connection=connection) as session:
                queue = create_destination(name=QUEUE_NAME)

                with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:

                    # Background thread: stop the consumer as soon as either
                    # the user presses Ctrl+C or the connection is lost.
                    def _watch():
                        while not connection_lost.is_set() and not shutdown.is_set():
                            time.sleep(0.1)
                        consumer.stop()

                    threading.Thread(target=_watch, daemon=True).start()

                    print(f"Connected — consuming from '{QUEUE_NAME}'. Press Ctrl+C to stop.")
                    for msg in consumer:
                        print(f"Received: {msg.body}")
                        for prop in msg.properties:
                            print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

    except Exception as e:
        print(f"[EMS] Session error: {e}")
        connection_lost.set()

    return shutdown.is_set()  # True → clean exit, False → reconnect


def main():
    shutdown = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown.set())

    retry = 0
    while retry <= MAX_RETRIES:
        clean_exit = consume(shutdown)
        if clean_exit:
            print("Shutting down.")
            break

        retry += 1
        if retry > MAX_RETRIES:
            print(f"Exceeded {MAX_RETRIES} reconnect attempts. Giving up.")
            break

        print(f"Reconnecting in {RECONNECT_DELAY_S}s (attempt {retry}/{MAX_RETRIES})...")
        # Wait for the delay, but wake immediately if the user presses Ctrl+C.
        shutdown.wait(timeout=RECONNECT_DELAY_S)


if __name__ == '__main__':
    main()
