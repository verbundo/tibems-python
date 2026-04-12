# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Publish a bytes message to a queue.
# tibemsSession_CreateBytesMessage is used instead of tibemsTextMsg_Create,
# and tibemsBytesMsg_WriteBytes writes the raw payload.
# A session handle must be passed to tibems_message when message_body is bytes.

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    tibems_connection,
    tibems_session,
    tibems_message,
    create_destination,
    create_producer,
    publish_message,
)

if __name__ == '__main__':
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
    ) as connection:

        with tibems_session(connection=connection) as session:

            queue = create_destination(name="tmp.q")
            producer = create_producer(session, queue)

            payload = b'\x00\x01\x02\x03Hello in bytes\xff\xfe'

            # session= is required when message_body is bytes
            with tibems_message(message_body=payload, session=session) as message:
                message_id, _ = publish_message(producer, message=message)
                print(f"Sent {len(payload)} bytes, message ID: {message_id}")
