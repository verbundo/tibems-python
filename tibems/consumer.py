# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Consumer-side helpers: TibEMSConsumer (sync, polling), AsyncTibEMSConsumer
# (push-based via SetMsgListener), and the create_consumer / create_async_consumer
# factory functions.

import asyncio
import ctypes
from ctypes import CFUNCTYPE, c_char_p, c_void_p, byref, c_bool, c_long, c_int, c_longlong

from .jms_properties import get_properties_with_types
from .tibems import (
    ems_lib,
    TIBEMS_OK,
    AckMode,
    ReceivedMessage,
    ReplyTo,
    TibEMSCreateConsumerError,
)

_RECEIVE_TIMEOUT_MS = 200  # polling interval used by __next__ to allow signal delivery

_TIBEMS_BYTES_MESSAGE = 1  # tibemsMessageType enum value for BytesMessage
_TIBEMS_TEXT_MESSAGE  = 5  # tibemsMessageType enum value for TextMessage

ems_lib.tibemsMsg_GetBodyType.argtypes      = [c_void_p, ctypes.POINTER(c_int)]
ems_lib.tibemsMsg_GetBodyType.restype       = c_int
ems_lib.tibemsBytesMsg_GetBodyLength.argtypes = [c_void_p, ctypes.POINTER(c_longlong)]
ems_lib.tibemsBytesMsg_GetBodyLength.restype  = c_int
ems_lib.tibemsBytesMsg_ReadBytes.argtypes   = [c_void_p, c_char_p, c_int, ctypes.POINTER(c_int)]
ems_lib.tibemsBytesMsg_ReadBytes.restype    = c_int


def _read_message_body(message) -> tuple[str, bytes | None]:
    """Return (text_body, bytes_body) for a received EMS message.

    Exactly one of the two values will be populated depending on the message type;
    the other will be an empty string or None respectively.
    """
    body_type = c_int()
    ems_lib.tibemsMsg_GetBodyType(message, byref(body_type))

    if body_type.value == _TIBEMS_BYTES_MESSAGE:
        length = c_longlong()
        ems_lib.tibemsBytesMsg_GetBodyLength(message, byref(length))
        if length.value <= 0:
            return ("", b"")
        buf = ctypes.create_string_buffer(length.value)
        bytes_read = c_int()
        ems_lib.tibemsBytesMsg_ReadBytes(message, buf, length.value, byref(bytes_read))
        return ("", buf.raw[:bytes_read.value])

    text_ptr = c_char_p()
    ems_lib.tibemsTextMsg_GetText(message, byref(text_ptr))
    return (text_ptr.value.decode("utf-8") if text_ptr.value else "", None)


# Callback type for tibemsMsgConsumer_SetMsgListener:
# void fn(tibemsMsgConsumer consumer, tibemsMsg message, void* closure)
_MsgListenerCallback = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)


def _get_reply_to(message) -> 'ReplyTo | None':
    dest = c_void_p()
    if ems_lib.tibemsMsg_GetReplyTo(message, byref(dest)) != TIBEMS_OK or not dest.value:
        return None
    # Name-getter functions (tibemsDestination_GetName, tibemsTempDestination_GetName)
    # segfault when called on handles returned by tibemsMsg_GetReplyTo.
    # The handle is still valid for use as a producer destination.
    return ReplyTo(name="", handle=dest)


class TibEMSConsumer:
    def __init__(self, session, destination, ack_mode: AckMode, selector: str = None, no_local: bool = False):
        self._session = session
        self._destination = destination
        self._ack_mode = ack_mode
        self._selector = selector
        self._no_local = no_local
        self._consumer = None
        self._current_msg_handle = None  # kept alive until next receive so acknowledge() remains valid
        self._running = True

    def stop(self):
        """Signal the consumer to stop after the current receive poll returns."""
        self._running = False

    def __enter__(self):
        self._consumer = c_void_p()
        selector_bytes = self._selector.encode() if self._selector else None
        res = ems_lib.tibemsSession_CreateConsumer(
            self._session, byref(self._consumer),
            self._destination,
            c_char_p(selector_bytes),
            c_bool(self._no_local)
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
        # Destroy the previous handle now that the caller has finished with it
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
        message_id = msg_id_ptr.value.decode('utf-8') if msg_id_ptr.value else None

        reply_to = _get_reply_to(message)
        if reply_to is not None:
            properties.append({"name": "JMSReplyTo", "type": "STRING", "value": reply_to.name or "<temporary queue>"})

        self._current_msg_handle = message

        if self._ack_mode == AckMode.TIBEMS_AUTO_ACK:
            ack_fn = lambda: None  # EMS acknowledges automatically on receive
        else:
            ack_fn = lambda: ems_lib.tibemsMsg_Acknowledge(message)

        return ReceivedMessage(body=body, body_bytes=body_bytes, properties=properties, ack_fn=ack_fn, reply_to=reply_to, message_id=message_id)

    def _destroy_current(self):
        if self._current_msg_handle is not None:
            ems_lib.tibemsMsg_Destroy(self._current_msg_handle)
            self._current_msg_handle = None


def create_consumer(session, destination, ack_mode: AckMode, selector: str = None, no_local: bool = False) -> TibEMSConsumer:
    return TibEMSConsumer(session, destination, ack_mode, selector, no_local)


class AsyncTibEMSConsumer:
    """Push-based async consumer using tibemsMsgConsumer_SetMsgListener.

    EMS delivers messages via a C callback on its own internal thread.
    Messages are forwarded to an asyncio.Queue and exposed as an async iterator,
    so no polling loop is needed and CPU usage is zero while the queue is idle.

    Usage::

        async with AsyncTibEMSConsumer(session, destination, AckMode.TIBEMS_AUTO_ACK) as consumer:
            async for msg in consumer:
                print(msg.body)
    """

    def __init__(self, session, destination, ack_mode: AckMode):
        self._session = session
        self._destination = destination
        self._ack_mode = ack_mode
        self._consumer = None
        self._callback = None  # must stay alive for the lifetime of the consumer
        self._msg_queue: asyncio.Queue | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def __aenter__(self):
        self._loop = asyncio.get_running_loop()
        self._msg_queue = asyncio.Queue()

        self._consumer = c_void_p()
        res = ems_lib.tibemsSession_CreateConsumer(
            self._session, byref(self._consumer),
            self._destination, c_char_p(None), c_bool(False)
        )
        if res != TIBEMS_OK:
            raise TibEMSCreateConsumerError(res)

        def _on_message(consumer_handle, message, closure):
            # Runs on an EMS internal C thread.
            # CFUNCTYPE delivers c_void_p args as plain Python ints (raw addresses).
            # Wrap back into c_void_p so ctypes treats them as full 64-bit pointers.
            try:
                message = c_void_p(message)
                # Extract all data now — the handle may be invalid after this returns.
                body, body_bytes = _read_message_body(message)
                properties = get_properties_with_types(ems_lib, message)

                msg_id_ptr = c_char_p()
                ems_lib.tibemsMsg_GetMessageID(message, byref(msg_id_ptr))
                message_id = msg_id_ptr.value.decode('utf-8') if msg_id_ptr.value else None

                reply_to = _get_reply_to(message)
                if reply_to is not None:
                    properties.append({"name": "JMSReplyTo", "type": "STRING", "value": reply_to.name or "<temporary queue>"})

                if self._ack_mode == AckMode.TIBEMS_CLIENT_ACK:
                    # Copy the message so its handle remains valid after this callback returns,
                    # allowing the caller to acknowledge it later from the async context.
                    msg_copy = c_void_p()
                    ems_lib.tibemsMsg_Copy(message, byref(msg_copy))
                    ack_fn = lambda: (ems_lib.tibemsMsg_Acknowledge(msg_copy), ems_lib.tibemsMsg_Destroy(msg_copy))
                else:
                    ack_fn = lambda: None

                received = ReceivedMessage(
                    body=body, body_bytes=body_bytes, properties=properties,
                    ack_fn=ack_fn, reply_to=reply_to, message_id=message_id
                )
                self._loop.call_soon_threadsafe(self._msg_queue.put_nowait, received)
            except Exception:
                # Swallow all exceptions — letting them propagate into the EMS C thread
                # would cause a segfault. The consumer will keep running; the lost
                # message is silently discarded.
                pass

        self._callback = _MsgListenerCallback(_on_message)
        ems_lib.tibemsMsgConsumer_SetMsgListener(self._consumer, self._callback, None)
        return self

    async def __aexit__(self, *_):
        # Signal any pending __anext__ callers to stop waiting
        self.stop()
        if self._consumer is not None:
            ems_lib.tibemsMsgConsumer_Close(self._consumer)
            self._consumer = None
        # Drain the queue so any remaining references are released
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


def create_async_consumer(session, destination, ack_mode: AckMode) -> AsyncTibEMSConsumer:
    return AsyncTibEMSConsumer(session, destination, ack_mode)
