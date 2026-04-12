# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Producer-side helpers: create_producer, publish_message, async_publish_message,
# and the one-shot convenience wrappers queue_publish / topic_publish.

import asyncio
import ctypes
from ctypes import CFUNCTYPE, c_char_p, c_void_p, byref, c_bool, c_long, c_int

from .jms_properties import get_properties_with_types
from .message import JmsPropertyType, JMS_Property, tibems_message
from .tibems import (
    ems_lib,
    TIBEMS_OK,
    AckMode,
    DestinationType,
    ReceivedMessage,
    tibems_connection,
    tibems_session,
    create_destination,
    TibEMSCreateProducerError,
    TibEMSPublishError,
    TibEMSConfigurationError,
    TibEMSCreateDestinationError,
)

# Callback for tibemsMsgProducer_AsyncSend:
# void fn(tibemsMsgProducer producer, tibemsMsg message, tibems_status status, void* closure)
# status == TIBEMS_OK (0) on success; non-zero on failure.
_AsyncSendCallback = CFUNCTYPE(None, c_void_p, c_void_p, c_int, c_void_p)

ems_lib.tibemsMsgProducer_AsyncSend.argtypes = [c_void_p, c_void_p, _AsyncSendCallback, c_void_p]
ems_lib.tibemsMsgProducer_AsyncSend.restype  = c_int

# Module-level dict that keeps each CFUNCTYPE wrapper alive until EMS fires the
# callback.  ctypes does NOT automatically retain Python references to callbacks
# passed as function arguments, so without this the GC can free the trampoline
# while EMS still holds a pointer to it — producing a segfault on the EMS thread.
_pending_async_sends: dict = {}


def create_producer(session, destination):
    producer = c_void_p()
    res = ems_lib.tibemsSession_CreateProducer(session, byref(producer), destination)
    if res != TIBEMS_OK:
        raise TibEMSCreateProducerError(res)
    return producer


def publish_message(producer, message, expect_reply: bool = False, reply_destination=None, session=None, reply_timeout: int | None = None) -> 'tuple[str, ReceivedMessage | None]':
    if expect_reply and session is None:
        raise TibEMSConfigurationError("'session' is required when expect_reply=True")

    if expect_reply:
        if reply_destination is not None:
            reply_dest = reply_destination
        else:
            reply_dest = c_void_p()
            res = ems_lib.tibemsSession_CreateTemporaryQueue(session, byref(reply_dest))
            if res != TIBEMS_OK:
                raise TibEMSCreateDestinationError(res)
        ems_lib.tibemsMsg_SetReplyTo(message, reply_dest)

    res = ems_lib.tibemsMsgProducer_Send(producer, message)
    if res != TIBEMS_OK:
        raise TibEMSPublishError(res)

    # Retrieve the sent message's JMSMessageID (assigned by the broker on send)
    msg_id_ptr = c_char_p()
    ems_lib.tibemsMsg_GetMessageID(message, byref(msg_id_ptr))
    message_id = msg_id_ptr.value.decode('utf-8') if msg_id_ptr.value else None

    if not expect_reply:
        return (message_id, None)

    consumer = c_void_p()
    ems_lib.tibemsSession_CreateConsumer(session, byref(consumer), reply_dest, c_char_p(None), c_bool(False))
    try:
        reply_msg = c_void_p()
        if reply_timeout is not None:
            ems_lib.tibemsMsgConsumer_ReceiveTimeout(consumer, byref(reply_msg), c_long(reply_timeout))
        else:
            ems_lib.tibemsMsgConsumer_Receive(consumer, byref(reply_msg))
        if not reply_msg.value:
            return (message_id, None)
        text_ptr = c_char_p()
        ems_lib.tibemsTextMsg_GetText(reply_msg, byref(text_ptr))
        body = text_ptr.value.decode('utf-8') if text_ptr.value else ""
        properties = get_properties_with_types(ems_lib, reply_msg)
        reply_id = next((p['value'] for p in properties if p['name'] == 'JMSMessageID'), None)
        ems_lib.tibemsMsg_Destroy(reply_msg)
        return (message_id, ReceivedMessage(body=body, properties=properties, ack_fn=lambda: None, reply_to=None, message_id=reply_id))
    finally:
        ems_lib.tibemsMsgConsumer_Close(consumer)
        # Destroy the temporary queue if we created one
        if reply_destination is None:
            ems_lib.tibemsSession_DeleteTemporaryQueue(session, reply_dest)


async def async_publish_message(producer, message) -> str:
    """Send a message asynchronously using tibemsMsgProducer_AsyncSend.

    EMS hands the send to its internal I/O thread and returns immediately.
    This coroutine suspends until the broker calls back (status == 0 → success,
    non-zero → TibEMSPublishError), then returns the JMSMessageID.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[None] = loop.create_future()

    def _on_async_send(prod_handle, msg_handle, status, closure):
        # Runs on an EMS internal C thread.
        _pending_async_sends.pop(send_key, None)
        if not future.done():
            if status == TIBEMS_OK:
                loop.call_soon_threadsafe(future.set_result, None)
            else:
                loop.call_soon_threadsafe(future.set_exception, TibEMSPublishError(status))

    callback = _AsyncSendCallback(_on_async_send)
    send_key = id(callback)
    _pending_async_sends[send_key] = callback  # keep the trampoline alive until EMS fires it

    res = ems_lib.tibemsMsgProducer_AsyncSend(producer, message, callback, None)
    if res != TIBEMS_OK:
        _pending_async_sends.pop(send_key, None)
        raise TibEMSPublishError(res)

    try:
        await future
    except Exception:
        _pending_async_sends.pop(send_key, None)
        raise

    # Read JMSMessageID from the original handle on the Python thread after
    # the broker has acknowledged the send.
    msg_id_ptr = c_char_p()
    ems_lib.tibemsMsg_GetMessageID(message, byref(msg_id_ptr))
    return msg_id_ptr.value.decode('utf-8') if msg_id_ptr.value else None


def queue_publish(ems_url: str, ems_username: str, ems_password: str, queue_name: str, message_txt: str, jms_props: list[JMS_Property] | None = None) -> str:
    if jms_props is None:
        jms_props = []
    with tibems_connection(url=ems_url, username=ems_username, password=ems_password) as connection:
        with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
            queue = create_destination(name=queue_name, dest_type=DestinationType.Queue)
            producer = create_producer(session, queue)
            with tibems_message(message_body=message_txt, jms_props=jms_props) as message:
                message_id, _ = publish_message(producer, message=message)
    return message_id


def topic_publish(ems_url: str, ems_username: str, ems_password: str, topic_name: str, message_txt: str, jms_props: list[JMS_Property] | None = None) -> str:
    if jms_props is None:
        jms_props = []
    with tibems_connection(url=ems_url, username=ems_username, password=ems_password) as connection:
        with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
            topic = create_destination(name=topic_name, dest_type=DestinationType.Topic)
            producer = create_producer(session, topic)
            with tibems_message(message_body=message_txt, jms_props=jms_props) as message:
                message_id, _ = publish_message(producer, message=message)
    return message_id
