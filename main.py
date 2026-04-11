import os
from tibems import JmsPropertyType, JMS_Property, AckMode, DestinationTye, tibems_connection, tibems_session, tibems_message, create_destination, create_producer, publish_message
from utils import load_dotenv

if __name__ == '__main__':
    load_dotenv()

    with tibems_connection(
        url=os.getenv("TIBEMS_URL"),
        username=os.getenv("TIBEMS_USER"),
        password=os.getenv("TIBEMS_PASSWORD"),
        start_connection=True,
        server_cert="certs/DekaBank_root_CA.pem",
        verify_server_cert=True
    ) as connection:
        with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:

            # publish message to queue
            queue = create_destination(name="tmp.q", type=DestinationTye.Queue)
            producer = create_producer(session, queue)
            with tibems_message(
                message_text="Test message",
                jms_props=[
                    JMS_Property(name="custom.prop", value="tmp.q", type=JmsPropertyType.String),
                    JMS_Property(name="custom.boolean.prop", value=False, type=JmsPropertyType.Boolean),
                    JMS_Property(name="custom.int.prop", value=1, type=JmsPropertyType.Integer),
                    JMS_Property(name="custom.float.prop", value=1.05, type=JmsPropertyType.Float)
                ]
            ) as message:
                reply = publish_message(producer, message=message, expect_reply=True, session=session, reply_timeout=5000)
                print("Successfully published a message to EMS queue")
                if reply:
                    print(f"Reply received: {reply.body}")
                    for prop in reply.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

            # publish message to topic
            topic = create_destination(name="t.test", type=DestinationTye.Topic)
            producer = create_producer(session, topic)
            with tibems_message(
                message_text="Test message",
                jms_props=[
                    JMS_Property(name="custom.prop", value="test", type=JmsPropertyType.String),
                    JMS_Property(name="custom.boolean.prop", value=False, type=JmsPropertyType.Boolean),
                    JMS_Property(name="custom.int.prop", value=1, type=JmsPropertyType.Integer),
                    JMS_Property(name="custom.float.prop", value=1.05, type=JmsPropertyType.Float)
                ]
            ) as message:
                publish_message(producer, message=message)
                print("Successfully published a message to EMS topic")
