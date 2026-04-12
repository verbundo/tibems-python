# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)

import os
from tibems import (
    JmsPropertyType,
    JMS_Property,
    DestinationType,
    tibems_connection, 
    tibems_session, 
    tibems_message, 
    create_destination, 
    create_producer, 
    publish_message
)
from utils import load_dotenv

if __name__ == '__main__':
    # load env vars from .env file
    load_dotenv()

    # create a connection, use env vars to connect to EMS
    with tibems_connection(
        url=os.getenv("TIBEMS_URL"),
        username=os.getenv("TIBEMS_USER"),
        password=os.getenv("TIBEMS_PASSWORD"),
        start_connection=True,
        server_cert="certs/root_CA.pem",
        verify_server_cert=True
    ) as connection:

        # create a session with default parameters (non-transacted, auto-acknowledge)
        with tibems_session(connection=connection) as session:

            # create a queue destination and a producer for that destination
            # 'type=DestinationType.Queue' is optional here, as 'Queue' is the default destination type
            queue = create_destination(name="tmp.q", dest_type=DestinationType.Queue)
            producer = create_producer(session, queue)

            # create a message with some custom JMS properties
            with tibems_message(
                message_body="Test message",
                jms_props=[
                    JMS_Property(name="custom.prop", value="tmp.q", type=JmsPropertyType.String),
                    JMS_Property(name="custom.boolean.prop", value=False, type=JmsPropertyType.Boolean),
                    JMS_Property(name="custom.int.prop", value=1, type=JmsPropertyType.Integer),
                    JMS_Property(name="custom.long.prop", value=1_000_000, type=JmsPropertyType.Long),
                    JMS_Property(name="custom.double.prop", value=1_000_000.123, type=JmsPropertyType.Double),
                    JMS_Property(name="custom.float.prop", value=1.05, type=JmsPropertyType.Float)
                ]
            ) as message:

                # publish message to queue, no response expected
                message_id, _ = publish_message(producer, message=message)
                print(f"Successfully published a message to EMS queue with ID: {message_id}")

                # publish message to queue, await a response on tmp queue
                message_id, reply = publish_message(producer, message=message, expect_reply=True, session=session, reply_timeout=5000)
                print(f"Successfully published a message to EMS queue with ID: {message_id}")
                if reply:
                    print(f"Reply received: {reply.body}")
                    for prop in reply.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

            # continue using a session to publish more messages

            # publish message to topic
            # 'type=DestinationType.Topic' is required here, as the default destination type is 'Queue'
            topic = create_destination(name="t.test", dest_type=DestinationType.Topic)
            producer = create_producer(session, topic)
            with tibems_message(
                message_body="Test message",
                jms_props=[
                    JMS_Property(name="custom.prop", value="test", type=JmsPropertyType.String),
                    JMS_Property(name="custom.boolean.prop", value=False, type=JmsPropertyType.Boolean),
                    JMS_Property(name="custom.int.prop", value=1, type=JmsPropertyType.Integer),
                    JMS_Property(name="custom.float.prop", value=1.05, type=JmsPropertyType.Float)
                ]
            ) as message:
                message_id, _ = publish_message(producer, message=message)
                print(f"Successfully published a message to EMS topic with ID: {message_id}")
