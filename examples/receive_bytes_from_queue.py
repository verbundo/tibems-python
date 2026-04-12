# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Consume bytes messages from a queue.
# The consumer auto-detects the message type via tibemsMsg_GetBodyType.
# For bytes messages msg.body_bytes contains the raw payload;
# for text messages msg.body contains the decoded string.

import sys
import os
import signal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    AckMode,
    tibems_connection,
    tibems_session,
    create_destination,
    create_consumer,
)

if __name__ == '__main__':
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
        start_connection=True,
    ) as connection:

        with tibems_session(connection=connection) as session:

            queue = create_destination(name="tmp.q")
            with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:
                signal.signal(signal.SIGINT, lambda *_: consumer.stop())
                print("Listening on tmp.q — press Ctrl+C to stop")

                for msg in consumer:
                    if msg.body_bytes is not None:
                        print(f"Received bytes message: {len(msg.body_bytes)} bytes")
                        print(f"  raw: {msg.body_bytes!r}")
                    else:
                        print(f"Received text message: {msg.body!r}")

                    if msg.properties:
                        print("JMS Properties:")
                        for prop in msg.properties:
                            print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")
