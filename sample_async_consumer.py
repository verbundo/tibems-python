# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)

import os
import asyncio
import signal
from utils import load_dotenv

from tibems import (
    AckMode,
    tibems_connection,
    tibems_session,
    tibems_message,
    create_destination,
    create_producer,
    create_async_consumer,
    publish_message,
)

async def main():
    load_dotenv()
    with tibems_connection(
        url=os.getenv("TIBEMS_URL"), 
        username=os.getenv("TIBEMS_USER"), 
        password=os.getenv("TIBEMS_PASSWORD"),
        start_connection=True,
    ) as connection:

        with tibems_session(connection=connection) as session:

            queue = create_destination(name="tmp.q")

            # AsyncTibEMSConsumer uses tibemsMsgConsumer_SetMsgListener so EMS
            # pushes messages via a C callback — no polling loop, zero CPU when idle.
            async with create_async_consumer(session, queue, AckMode.TIBEMS_AUTO_ACK) as consumer:
                loop = asyncio.get_running_loop()
                # stop the consumer on Ctrl+C
                loop.add_signal_handler(signal.SIGINT, consumer.stop)

                print("Listening on tmp.q — press Ctrl+C to stop")
                async for msg in consumer:
                    print(f"Received: {msg.body}")
                    print("JMS Properties:")
                    for prop in msg.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

                    if msg.reply_to:
                        print(f"Reply-To handle available, sending reply")
                        reply_producer = create_producer(session, msg.reply_to.handle)
                        with tibems_message(message_body="Reply", jms_props=[], correlation_id=msg.message_id) as reply:
                            reply_id, _ = publish_message(reply_producer, reply)
                            print(f"Reply sent, message ID: {reply_id}")

if __name__ == '__main__':
    asyncio.run(main())
