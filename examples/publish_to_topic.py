# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)

import sys
import os

# add parent directory to sys.path to allow imports from the root of the project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    DestinationType,
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
    # create a connection
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
    ) as connection:

        # create a session with default parameters (non-transacted, auto-acknowledge)
        with tibems_session(connection=connection) as session:

            # create a topic destination and a producer for that destination
            # 'type=DestinationType.Topic' is required here, as the default destination type is 'Queue'
            topic = create_destination(name="t.test", type=DestinationType.Topic)
            producer = create_producer(session, topic)

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
                # publish message to topic
                message_id, _ = publish_message(producer, message=message)
                print(f"Successfully published a message to EMS topic, message ID: {message_id}")
            
            # continue using a session to publish more messages
            # create more destinations, publishers, messages, if needed