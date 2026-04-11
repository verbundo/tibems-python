import sys
import os
import signal

# add parent directory to sys.path to allow imports from the root of the project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    AckMode,
    tibems_connection,
    tibems_session,
    create_destination,
    create_consumer,
)

if __name__ == '__main__':
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
        start_connection=True,
    ) as connection:

        # CLIENT_ACK mode: messages are not removed from the queue until msg.acknowledge() is called.
        # This lets the consumer control exactly when a message is considered processed.
        # If the consumer crashes before acknowledging, EMS will redeliver the message.
        with tibems_session(connection=connection, ack_mode=AckMode.TIBEMS_CLIENT_ACK) as session:

            queue = create_destination(name="tmp.q")
            with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_CLIENT_ACK) as consumer:
                signal.signal(signal.SIGINT, lambda *_: consumer.stop())
                print("Listening on tmp.q with CLIENT_ACK — press Ctrl+C to stop")

                for msg in consumer:
                    try:
                        print(f"Received: {msg.body}")
                        print("JMS Properties:")
                        for prop in msg.properties:
                            print(f"  - {prop['name']} ({prop['type']}): {prop['value']}")

                        # do processing work here — only acknowledge after successful handling
                        msg.acknowledge()
                        print("Message acknowledged")
                    except Exception as e:
                        # not acknowledging means EMS will redeliver this message
                        print(f"Processing failed, message will be redelivered: {e}")
