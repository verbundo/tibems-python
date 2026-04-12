# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Demonstrates tibemsMsgProducer_AsyncSend via async_publish_message.
# EMS hands the send to its internal I/O thread; the coroutine suspends until
# the broker acknowledges the send, then prints the JMSMessageID.

import asyncio
import os
from utils import load_dotenv

from tibems import (
    JmsPropertyType,
    JMS_Property,
    DestinationType,
    tibems_connection,
    tibems_session,
    tibems_message,
    create_destination,
    create_producer,
    async_publish_message,
)


async def main():
    # load env vars from .env file
    load_dotenv()

    with tibems_connection(
        url=os.getenv("TIBEMS_URL"),
        username=os.getenv("TIBEMS_USER"),
        password=os.getenv("TIBEMS_PASSWORD"),
    ) as connection:
        with tibems_session(connection=connection) as session:
            queue = create_destination(name="tmp.q", type=DestinationType.Queue)
            producer = create_producer(session, queue)

            # Send five messages concurrently using AsyncSend.
            # Each send is a separate coroutine suspended until the broker ACKs.
            async def send(text, index):
                with tibems_message(
                    message_body=text,
                    jms_props=[
                        JMS_Property(name="msg.index", value=index, type=JmsPropertyType.Integer),
                    ],
                ) as message:
                    message_id = await async_publish_message(producer, message)
                    print(f"[{index}] sent, JMSMessageID={message_id}")

            await asyncio.gather(
                send("Hello from async send 1", 1),
                send("Hello from async send 2", 2),
                send("Hello from async send 3", 3),
                send("Hello from async send 4", 4),
                send("Hello from async send 5", 5),
            )


if __name__ == '__main__':
    asyncio.run(main())
