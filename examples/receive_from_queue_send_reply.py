import sys
import os
import signal

# add parent directory to sys.path to allow imports from the root of the project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tibems import (
    AckMode,
    tibems_connection,
    tibems_session,
    tibems_message,
    create_destination,
    create_consumer,
    create_producer,
    publish_message,
)

if __name__ == '__main__':
    with tibems_connection(
        url='tcp://localhost:7222',
        username='test_user',
        password='test_password',
        start_connection=True,
    ) as connection:

        with tibems_session(connection=connection) as session:

            queue = create_destination(name="tmp.q")
            with create_consumer(session, queue, ack_mode=AckMode.TIBEMS_AUTO_ACK) as consumer:
                signal.signal(signal.SIGINT, lambda *_: consumer.stop())
                print("Listening on tmp.q, will reply to JMSReplyTo — press Ctrl+C to stop")

                for msg in consumer:
                    print(f"Received: {msg.body}")

                    if msg.reply_to:
                        # create a producer targeting the reply-to destination from the request
                        reply_producer = create_producer(session, msg.reply_to.handle)
                        with tibems_message(
                            message_text=f"Reply to: {msg.body}",
                            jms_props=[],
                            correlation_id=msg.message_id,  # correlate reply to original request
                        ) as reply:
                            reply_id, _ = publish_message(reply_producer, reply)
                            print(f"Reply sent, message ID: {reply_id}")
                    else:
                        print("No JMSReplyTo set on message — skipping reply")
