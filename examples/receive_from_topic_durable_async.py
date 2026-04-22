# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Async durable topic subscriber — push-based delivery via SetMsgListener;
# messages published while offline are retained and delivered on reconnect.

import sys
import os
import asyncio
import signal

# add parent directory to sys.path to allow imports from the root of the project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    AckMode,
    DestinationType,
    tibems_connection,
    tibems_session,
    create_destination,
    create_async_durable_subscriber,
)

TOPIC_NAME = "t.test"
SUBSCRIBER_NAME = "my-durable-sub"
CLIENT_ID = "my-app-client"

async def main():
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
        start_connection=True,   # required for receiving messages
        client_id=CLIENT_ID,     # required for durable subscriptions
    ) as connection:

        with tibems_session(connection=connection) as session:

            topic = create_destination(name=TOPIC_NAME, dest_type=DestinationType.Topic)

            async with create_async_durable_subscriber(
                session, topic, SUBSCRIBER_NAME, AckMode.TIBEMS_AUTO_ACK
            ) as sub:
                loop = asyncio.get_running_loop()
                # stop the subscriber on Ctrl+C (Unix only; use signal.signal on Windows)
                loop.add_signal_handler(signal.SIGINT, sub.stop)

                print(f"Async durable subscriber '{SUBSCRIBER_NAME}' on '{TOPIC_NAME}' — press Ctrl+C to stop")

                async for msg in sub:
                    print(f"Received: {msg.body}")
                    print("JMS Properties:")
                    for prop in msg.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

if __name__ == '__main__':
    asyncio.run(main())
