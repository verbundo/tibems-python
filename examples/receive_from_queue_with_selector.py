# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)

import sys
import os
import signal

# add parent directory to sys.path to allow imports from the root of the project
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

            # JMS selector uses SQL-92 syntax to filter messages by their properties.
            # Only messages where custom.prop = 'tmp.q' AND custom.int.prop > 0 will be delivered.
            # Messages that do not match the selector remain on the queue for other consumers.
            selector = "custom.prop = 'tmp.q' AND custom.int.prop > 0"

            with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_AUTO_ACK, selector=selector) as consumer:
                signal.signal(signal.SIGINT, lambda *_: consumer.stop())
                print(f"Listening on tmp.q with selector: {selector!r}")
                print("Press Ctrl+C to stop")

                for msg in consumer:
                    print(f"Received: {msg.body}")
                    print("JMS Properties:")
                    for prop in msg.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")
