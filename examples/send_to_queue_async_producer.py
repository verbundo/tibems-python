# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Demonstrates tibemsMsgProducer_AsyncSend via async_publish_message.
# EMS hands the send to its internal I/O thread; the coroutine suspends until
# the broker acknowledges the send, then prints the JMSMessageID.

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    AckMode,
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
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
    ) as connection:
        with tibems_session(connection=connection) as session:
            queue = create_destination(name="tmp.q", dest_type=DestinationType.Queue)
            producer = create_producer(session, queue)

            # Send three messages concurrently using AsyncSend.
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
            )


if __name__ == '__main__':
    asyncio.run(main())
