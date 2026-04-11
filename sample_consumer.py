import os
import signal
from tibems import AckMode, DestinationType, tibems_connection, tibems_session, create_destination, create_consumer, create_producer, tibems_message, publish_message
from utils import load_dotenv

if __name__ == '__main__':
    load_dotenv()

    with tibems_connection(
        url=os.getenv("TIBEMS_URL"), 
        username=os.getenv("TIBEMS_USER"), 
        password=os.getenv("TIBEMS_PASSWORD"),
        start_connection=True,
        server_cert="certs/root_CA.pem",
        verify_server_cert=True
    ) as connection:
        with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:

            # publish message to queue
            queue = create_destination(name="tmp.q", type=DestinationType.Queue)
            with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:
                signal.signal(signal.SIGINT, lambda *_: consumer.stop())
                for msg in consumer:
                    print(f"Received: {msg.body}")
                    print("JMS Properties:")
                    for prop in msg.properties:
                        print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

                    if msg.reply_to:
                        print(f"Reply-To handle available: {msg.reply_to}")
                        producer = create_producer(session, msg.reply_to.handle)
                        with tibems_message(message_text="Reply", jms_props=[], correlation_id=msg.message_id) as reply:
                            publish_message(producer, reply)

                    # msg.acknowledge() is required here when session uses CLIENT_ACK mode