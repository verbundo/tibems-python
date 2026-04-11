# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)

import sys
import os
import signal

# add parent directory to sys.path to allow imports from the root of the project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    AckMode,
    DestinationType,
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
        start_connection=True,  # required for receiving messages
    ) as connection:

        with tibems_session(connection=connection) as session:

            # 'type=DestinationType.Topic' is required here, as the default destination type is 'Queue'
            topic = create_destination(name="t.test", type=DestinationType.Topic)
            with create_consumer(session, topic, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:
                # stop the consumer loop on Ctrl+C
                signal.signal(signal.SIGINT, lambda *_: consumer.stop())
                print("Subscribed to t.test — press Ctrl+C to stop")

                for msg in consumer:
                    print(f"Received message: {msg.body}")
                    print("JMS Properties:")
                    for prop in msg.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")
