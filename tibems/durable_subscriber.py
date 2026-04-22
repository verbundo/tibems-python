# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Durable topic subscriber: wraps tibemsSession_CreateDurableSubscriber so
# that EMS retains messages published while the subscriber is offline.

import asyncio
import ctypes
from ctypes import CFUNCTYPE, c_char_p, c_void_p, byref, c_bool, c_long

from .consumer import _MsgListenerCallback, _read_message_body, _get_reply_to, _RECEIVE_TIMEOUT_MS
from .jms_properties import get_properties_with_types
from .tibems import (
    ems_lib,
    TIBEMS_OK,
    AckMode,
    ReceivedMessage,
    TibEMSCreateConsumerError,
    get_ems_message,
)


class TibEMSUnsubscribeError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error unsubscribing durable subscription: {status} - {msg}")


class DurableSubscriber:
    """Durable topic subscriber using ``tibemsSession_CreateDurableSubscriber``.

    Unlike a regular topic consumer, a durable subscriber retains messages
    published to the topic while the subscriber is inactive, so no messages
    are missed between reconnects.

    The connection used to create this subscriber **must** have a client ID
    set (via ``client_id=`` on :func:`tibems_connection`); EMS requires a
    client ID to persist durable subscription state across reconnects.

    Usage::

        with create_durable_subscriber(
            session, topic, "my-sub", AckMode.TIBEMS_AUTO_ACK
        ) as sub:
            signal.signal(signal.SIGINT, lambda *_: sub.stop())
            for msg in sub:
                print(msg.body)
    """

    def __init__(
        self,
        session,
        topic,
        subscriber_name: str,
        ack_mode: AckMode,
        selector: str = None,
        no_local: bool = False,
    ):
        self._session = session
        self._topic = topic
        self._subscriber_name = subscriber_name
        self._ack_mode = ack_mode
        self._selector = selector
        self._no_local = no_local
        self._consumer = None
        self._current_msg_handle = None
        self._running = True

    def stop(self):
        """Signal the subscriber to stop after the current receive poll returns."""
        self._running = False

    def __enter__(self):
        self._consumer = c_void_p()
        selector_bytes = self._selector.encode() if self._selector else None
        res = ems_lib.tibemsSession_CreateDurableSubscriber(
            self._session,
            byref(self._consumer),
            self._topic,
            self._subscriber_name.encode(),
            c_char_p(selector_bytes),
            c_bool(self._no_local),
        )
        if res != TIBEMS_OK:
            raise TibEMSCreateConsumerError(res)
        return self

    def __exit__(self, *_):
        self._destroy_current()
        if self._consumer is not None:
            ems_lib.tibemsMsgConsumer_Close(self._consumer)
            self._consumer = None

    def __iter__(self):
        return self

    def __next__(self) -> ReceivedMessage:
        self._destroy_current()

        while self._running:
            message = c_void_p()
            status = ems_lib.tibemsMsgConsumer_ReceiveTimeout(
                self._consumer, byref(message), c_long(_RECEIVE_TIMEOUT_MS)
            )
            if status == TIBEMS_OK and message.value:
                break
        else:
            raise StopIteration

        body, body_bytes = _read_message_body(message)
        properties = get_properties_with_types(ems_lib, message)

        msg_id_ptr = c_char_p()
        ems_lib.tibemsMsg_GetMessageID(message, byref(msg_id_ptr))
        message_id = msg_id_ptr.value.decode("utf-8") if msg_id_ptr.value else None

        reply_to = _get_reply_to(message)
        if reply_to is not None:
            properties.append({"name": "JMSReplyTo", "type": "STRING", "value": reply_to.name or "<temporary queue>"})

        self._current_msg_handle = message

        if self._ack_mode == AckMode.TIBEMS_AUTO_ACK:
            ack_fn = lambda: None
        else:
            ack_fn = lambda: ems_lib.tibemsMsg_Acknowledge(message)

        return ReceivedMessage(
            body=body,
            body_bytes=body_bytes,
            properties=properties,
            ack_fn=ack_fn,
            reply_to=reply_to,
            message_id=message_id,
        )

    def _destroy_current(self):
        if self._current_msg_handle is not None:
            ems_lib.tibemsMsg_Destroy(self._current_msg_handle)
            self._current_msg_handle = None


def create_durable_subscriber(
    session,
    topic,
    subscriber_name: str,
    ack_mode: AckMode,
    selector: str = None,
    no_local: bool = False,
) -> DurableSubscriber:
    """Create a durable topic subscriber.

    Args:
        session: A session handle from :func:`tibems_session`.
        topic: A topic destination handle from :func:`create_destination`.
        subscriber_name: Unique name for the durable subscription. EMS uses
            this name (combined with the connection's client ID) to identify
            the subscription across reconnects.
        ack_mode: Acknowledgment mode for received messages.
        selector: Optional JMS selector (SQL-92 syntax) to filter messages.
        no_local: If ``True``, suppress delivery of messages published by
            the same connection.

    Returns:
        A :class:`DurableSubscriber` context manager / iterator.
    """
    return DurableSubscriber(session, topic, subscriber_name, ack_mode, selector, no_local)


class AsyncDurableSubscriber:
    """Push-based async durable topic subscriber using ``tibemsMsgConsumer_SetMsgListener``.

    Combines the durability of :class:`DurableSubscriber` (messages retained
    while offline) with the push-based, zero-polling delivery of
    ``AsyncTibEMSConsumer``.  EMS delivers messages via a C callback on its
    own internal thread; they are forwarded to an ``asyncio.Queue`` and
    exposed as an async iterator.

    The connection **must** have a ``client_id`` set.

    Usage::

        async with create_async_durable_subscriber(
            session, topic, "my-sub", AckMode.TIBEMS_AUTO_ACK
        ) as sub:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, sub.stop)
            async for msg in sub:
                print(msg.body)
    """

    def __init__(
        self,
        session,
        topic,
        subscriber_name: str,
        ack_mode: AckMode,
        selector: str = None,
        no_local: bool = False,
    ):
        self._session = session
        self._topic = topic
        self._subscriber_name = subscriber_name
        self._ack_mode = ack_mode
        self._selector = selector
        self._no_local = no_local
        self._consumer = None
        self._callback = None  # must stay alive for the lifetime of the consumer
        self._msg_queue: asyncio.Queue | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def __aenter__(self):
        self._loop = asyncio.get_running_loop()
        self._msg_queue = asyncio.Queue()

        self._consumer = c_void_p()
        selector_bytes = self._selector.encode() if self._selector else None
        res = ems_lib.tibemsSession_CreateDurableSubscriber(
            self._session,
            byref(self._consumer),
            self._topic,
            self._subscriber_name.encode(),
            c_char_p(selector_bytes),
            c_bool(self._no_local),
        )
        if res != TIBEMS_OK:
            raise TibEMSCreateConsumerError(res)

        def _on_message(consumer_handle, message, closure):
            try:
                message = c_void_p(message)
                body, body_bytes = _read_message_body(message)
                properties = get_properties_with_types(ems_lib, message)

                msg_id_ptr = c_char_p()
                ems_lib.tibemsMsg_GetMessageID(message, byref(msg_id_ptr))
                message_id = msg_id_ptr.value.decode("utf-8") if msg_id_ptr.value else None

                reply_to = _get_reply_to(message)
                if reply_to is not None:
                    properties.append({"name": "JMSReplyTo", "type": "STRING", "value": reply_to.name or "<temporary queue>"})

                if self._ack_mode == AckMode.TIBEMS_CLIENT_ACK:
                    msg_copy = c_void_p()
                    ems_lib.tibemsMsg_Copy(message, byref(msg_copy))
                    ack_fn = lambda: (ems_lib.tibemsMsg_Acknowledge(msg_copy), ems_lib.tibemsMsg_Destroy(msg_copy))
                else:
                    ack_fn = lambda: None

                received = ReceivedMessage(
                    body=body, body_bytes=body_bytes, properties=properties,
                    ack_fn=ack_fn, reply_to=reply_to, message_id=message_id,
                )
                self._loop.call_soon_threadsafe(self._msg_queue.put_nowait, received)
            except Exception:
                pass

        self._callback = _MsgListenerCallback(_on_message)
        ems_lib.tibemsMsgConsumer_SetMsgListener(self._consumer, self._callback, None)
        return self

    async def __aexit__(self, *_):
        self.stop()
        if self._consumer is not None:
            ems_lib.tibemsMsgConsumer_Close(self._consumer)
            self._consumer = None
        if self._msg_queue is not None:
            while not self._msg_queue.empty():
                try:
                    self._msg_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            self._msg_queue = None

    def stop(self):
        """Signal the async iterator to stop. Safe to call from any thread."""
        if self._loop is not None and self._msg_queue is not None:
            self._loop.call_soon_threadsafe(self._msg_queue.put_nowait, None)

    def __aiter__(self):
        return self

    async def __anext__(self) -> ReceivedMessage:
        msg = await self._msg_queue.get()
        if msg is None:
            raise StopAsyncIteration
        return msg


def create_async_durable_subscriber(
    session,
    topic,
    subscriber_name: str,
    ack_mode: AckMode,
    selector: str = None,
    no_local: bool = False,
) -> AsyncDurableSubscriber:
    """Create a push-based async durable topic subscriber.

    Args:
        session: A session handle from :func:`tibems_session`.
        topic: A topic destination handle from :func:`create_destination`.
        subscriber_name: Unique name for the durable subscription.
        ack_mode: Acknowledgment mode for received messages.
        selector: Optional JMS selector (SQL-92 syntax) to filter messages.
        no_local: If ``True``, suppress delivery of messages published by
            the same connection.

    Returns:
        An :class:`AsyncDurableSubscriber` async context manager / async iterator.
    """
    return AsyncDurableSubscriber(session, topic, subscriber_name, ack_mode, selector, no_local)


def unsubscribe(session, subscriber_name: str) -> None:
    """Delete a named durable subscription from the EMS server.

    The subscriber must be closed (inactive) before calling this.
    After unsubscribing, queued messages for that subscription are discarded.

    Args:
        session: A session handle from :func:`tibems_session`.
        subscriber_name: The name used when the subscription was created.

    Raises:
        TibEMSUnsubscribeError: If the EMS server returns an error.
    """
    res = ems_lib.tibemsSession_Unsubscribe(session, subscriber_name.encode())
    if res != TIBEMS_OK:
        raise TibEMSUnsubscribeError(res)
