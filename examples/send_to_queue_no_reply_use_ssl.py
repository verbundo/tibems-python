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
    publish_message
)

if __name__ == '__main__':
    # create a connection over ssl
    # use certs/root_CA.pem to verify the server's identity
    with tibems_connection(
        url='ssl://localhost:7223',
        username='test_user',
        password='test_password',
        server_cert="certs/root_CA.pem",
    ) as connection:

        # create a session with default parameters (non-transacted, auto-acknowledge)
        with tibems_session(connection=connection) as session:

            # create a queue destination and a producer for that destination
            queue = create_destination(name="tmp.q")
            producer = create_producer(session, queue)

            # create a message with some custom JMS properties
            with tibems_message(
                message_text="Test message",
                jms_props=[
                    JMS_Property(name="custom.prop", value="tmp.q", type=JmsPropertyType.String),
                    JMS_Property(name="custom.boolean.prop", value=False, type=JmsPropertyType.Boolean),
                    JMS_Property(name="custom.int.prop", value=1, type=JmsPropertyType.Integer),
                    JMS_Property(name="custom.float.prop", value=1.05, type=JmsPropertyType.Float)
                ]
            ) as message:
                # publish message to queue, no reply expected
                message_id, _ = publish_message(producer, message=message)
                print(f"Successfully published a message to EMS queue, message ID: {message_id}")