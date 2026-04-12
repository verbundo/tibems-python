# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)

import os
import signal
from tibems import (
    AckMode,
    DestinationType,
    tibems_connection,
    tibems_session,
    tibems_message,
    create_destination,
    create_producer,
    create_consumer,
    publish_message,
    session_commit,
    session_rollback,
    get_message_body
)
from utils import load_dotenv

if __name__ == '__main__':
    # load env vars from .env file
    load_dotenv()

    # create a connection
    with tibems_connection(
        url=os.getenv("TIBEMS_URL"),
        username=os.getenv("TIBEMS_USER"),
        password=os.getenv("TIBEMS_PASSWORD"),
        start_connection=True,
        server_cert="certs/root_CA.pem",
        verify_server_cert=True
    ) as connection:

        # create a transacted session (transacted=True)
        with tibems_session(connection=connection, transacted=True) as session:

            queue1 = create_destination(name="tmp.q", dest_type=DestinationType.Queue)
            queue2 = create_destination(name="tmp2.q", dest_type=DestinationType.Queue)
            producer = create_producer(session, queue2)

            with create_consumer(session, queue1, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:
                signal.signal(signal.SIGINT, lambda *_: consumer.stop())

                for msg in consumer:
                    print(f"Received: {msg.body}")

                    try:
                        # process the message (could fail)
                        #process(msg.body)

                        # consume: publish a derived message in the same transaction
                        with tibems_message(message_body=f"Processed: {msg.body}") as reply:
                            text, _ = get_message_body(reply)
                            print(f"Publishing message: {text}")
                            publish_message(producer, message=reply)

                        # commit: both the receive-ack and the publish are committed atomically
                        session_commit(session)
                        print(f"  -> committed")

                    except Exception as e:
                        # rollback: the received message goes back to the queue,
                        # and no reply is published
                        print(f"  -> error: {e}, rolling back")
                        session_rollback(session)
