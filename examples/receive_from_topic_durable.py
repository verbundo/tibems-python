# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Durable topic subscriber — messages published while this subscriber is
# offline are retained by EMS and delivered on reconnect.
#
# To delete the subscription entirely (and discard any retained messages):
#
#   from tibems import tibems_connection, tibems_session, unsubscribe
#   with tibems_connection(..., client_id="my-app-client") as conn:
#       with tibems_session(conn) as session:
#           unsubscribe(session, "my-durable-sub")

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
    create_durable_subscriber,
)

TOPIC_NAME = "t.test"
SUBSCRIBER_NAME = "my-durable-sub"
CLIENT_ID = "my-app-client"

if __name__ == '__main__':
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
        start_connection=True,   # required for receiving messages
        client_id=CLIENT_ID,     # required for durable subscriptions
    ) as connection:

        with tibems_session(connection=connection) as session:

            topic = create_destination(name=TOPIC_NAME, dest_type=DestinationType.Topic)

            with create_durable_subscriber(
                session, topic, SUBSCRIBER_NAME, AckMode.TIBEMS_AUTO_ACK
            ) as sub:
                signal.signal(signal.SIGINT, lambda *_: sub.stop())
                print(f"Durable subscriber '{SUBSCRIBER_NAME}' on '{TOPIC_NAME}' — press Ctrl+C to stop")

                for msg in sub:
                    print(f"Received: {msg.body}")
                    print("JMS Properties:")
                    for prop in msg.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")
