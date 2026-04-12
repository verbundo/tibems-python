# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)

import sys
import os

# add parent directory to sys.path to allow imports from the root of the project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    JmsPropertyType,
    JMS_Property,
    tibems_connection,
    tibems_session,
    tibems_message,
    create_destination,
    create_producer,
    publish_message,
)

if __name__ == '__main__':
    # start_connection=True is required so that the temporary reply queue can receive
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
        start_connection=True,
    ) as connection:

        with tibems_session(connection=connection) as session:

            queue = create_destination(name="tmp.q")
            producer = create_producer(session, queue)

            with tibems_message(
                message_body="Request message",
                jms_props=[
                    JMS_Property(name="custom.prop", value="some-value", type=JmsPropertyType.String),
                ]
            ) as message:
                # publish_message creates a temporary reply queue, sets JMSReplyTo on the message,
                # sends it, then blocks until a reply arrives or the timeout expires
                message_id, reply = publish_message(
                    producer,
                    message=message,
                    expect_reply=True,
                    session=session,
                    reply_timeout=5000,  # milliseconds; pass None to block indefinitely
                )
                print(f"Request sent, message ID: {message_id}")

                if reply:
                    print(f"Reply received: {reply.body}")
                    print("Reply JMS Properties:")
                    for prop in reply.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")
                else:
                    print("No reply received within the timeout period")
