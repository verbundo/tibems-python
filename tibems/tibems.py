# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Python wrapper for the TIBCO Enterprise Message Service (EMS) C API.
# Uses ctypes to call into the native libtibems.so shared library, exposing
# Pythonic context managers for connections, sessions, messages, producers,
# and consumers.

from contextlib import contextmanager
import asyncio
import ctypes
from ctypes import CFUNCTYPE, c_char_p, c_void_p, byref, POINTER, c_bool, c_float, c_byte, c_short, c_double, c_long, c_int
from enum import Enum

from .jms_properties import get_properties_with_types, TYPE_MAP, tibemsMsgField, tibemsData

ems_lib = ctypes.CDLL("libtibems.so") 

TIBEMS_OK = 0
TIBEMS_FALSE = 0
TIBEMS_TRUE = 1

class AckMode(Enum):
    TIBEMS_AUTO_ACK = 1
    TIBEMS_CLIENT_ACK=2
    TIBEMS_DUPS_OK_ACK=3

TIBEMS_SSL_ENCODING_AUTO = 0

tibems_status = ctypes.c_int
tibems_bool = ctypes.c_int

ems_lib.tibems_Open.restype = None
ems_lib.tibemsConnectionFactory_Create.restype = c_void_p
ems_lib.tibemsConnectionFactory_SetServerURL.argtypes = [c_void_p, c_char_p]
ems_lib.tibemsConnectionFactory_CreateConnection.argtypes = [c_void_p, POINTER(c_void_p), c_char_p, c_char_p]

ems_lib.tibemsStatus_GetText.argtypes = [c_int]
ems_lib.tibemsStatus_GetText.restype = c_char_p

class JmsPropertyType(int, Enum):
    String=0
    Boolean=1
    Byte=2
    Short=3
    Integer=4
    Long=5
    Float=6
    Double=7

class DestinationType(int, Enum):
    Queue=0
    Topic=1

class ProtocolType(str, Enum):
    TCP="tcp"
    SSL="ssl"


class JMS_Property:
    def __init__(self, name: str, value: str|bool|int|float , type: JmsPropertyType=JmsPropertyType.String):
        self.name = name
        self.value = value
        self.type = type

@contextmanager
def tibems_connection(url: str, username: str, password: str, start_connection: bool=False, server_cert: str | None = None, client_cert: str | None = None, private_key: str | None = None, verify_server_cert: bool = True):
    ems_lib.tibems_Open()
    use_ssl: bool = url.startswith("ssl:")
    ssl_params = None
    if use_ssl:
        if not server_cert:
            if verify_server_cert:
                raise TibEMSConfigurationError("'server_cert' parameter must be specified for SSL connection")
        ssl_params = ems_lib.tibemsSSLParams_Create()
        if verify_server_cert:
            # add server trusted certificate
            ems_lib.tibemsSSLParams_AddTrustedCertFile(ssl_params, bytes(server_cert, "UTF-8"), TIBEMS_SSL_ENCODING_AUTO)
        else:
            ems_lib.tibemsSSLParams_SetVerifyHost(ssl_params, TIBEMS_FALSE)
            ems_lib.tibemsSSLParams_SetVerifyHostName(ssl_params, TIBEMS_FALSE)
        if client_cert:
            # path to client cert in p12 format
            ems_lib.tibemsSSLParams_SetIdentityFile(ssl_params, client_cert)
    factory = ems_lib.tibemsConnectionFactory_Create()
    if ssl_params:
        ems_lib.tibemsConnectionFactory_SetSSLParams(factory, ssl_params)
    ems_lib.tibemsConnectionFactory_SetServerURL(factory, bytes(url, "utf-8"))
    connection = c_void_p()
    res = ems_lib.tibemsConnectionFactory_CreateConnection(factory, byref(connection), bytes(username, "utf-8"), bytes(password, "utf-8"))
    if res != TIBEMS_OK:
        raise TibEMSConnectionError(res)
    if start_connection:
        ems_lib.tibemsConnection_Start(connection)
    try:
        yield connection
    finally:
        if ssl_params:
            ems_lib.tibemsSSLParams_Destroy(ssl_params)
        ems_lib.tibemsConnection_Close(connection)
        ems_lib.tibems_Close()

@contextmanager
def tibems_session(connection, transacted: bool = False, ack_mode: AckMode = AckMode.TIBEMS_AUTO_ACK):
    session = c_void_p()
    res = ems_lib.tibemsConnection_CreateSession(connection, byref(session), transacted, ack_mode.value)
    if res != TIBEMS_OK:
        raise TibEMSSessionError(res)
    try:
        yield session
    finally:
        ems_lib.tibemsSession_Close(session)

@contextmanager
def tibems_message(message_text: str, jms_props: list[JMS_Property], correlation_id: str | None = None):
    message = create_message(message_text=message_text, jms_props=jms_props, correlation_id=correlation_id)
    try:
        yield message
    finally:
        destroy_message(message=message)

def create_destination(name: str, type: int=DestinationType.Queue):
    destination = c_void_p()
    if type == DestinationType.Queue:
        res = ems_lib.tibemsQueue_Create(byref(destination), bytes(name, "utf-8"))
    else:
        res = ems_lib.tibemsTopic_Create(byref(destination), bytes(name, "utf-8"))
    if res != TIBEMS_OK:
        raise TibEMSCreateDestinationError(res)
    return destination

def create_producer(session, destination):
    producer = c_void_p()
    res = ems_lib.tibemsSession_CreateProducer(session, byref(producer), destination)
    if res != TIBEMS_OK:
        raise TibEMSCreateProducerError(res)
    return producer

class ReplyTo:
    """JMSReplyTo destination from a received message.
    The handle is valid until the next message is received from the consumer."""
    def __init__(self, name: str, handle: c_void_p):
        self.name = name
        self.handle = handle  # usable directly as a destination in create_producer()


def _get_reply_to(message) -> 'ReplyTo | None':
    dest = c_void_p()
    if ems_lib.tibemsMsg_GetReplyTo(message, byref(dest)) != TIBEMS_OK or not dest.value:
        return None
    # Name-getter functions (tibemsDestination_GetName, tibemsTempDestination_GetName)
    # segfault when called on handles returned by tibemsMsg_GetReplyTo.
    # The handle is still valid for use as a producer destination.
    return ReplyTo(name="", handle=dest)


class ReceivedMessage:
    def __init__(self, body: str, properties: list[dict], ack_fn, reply_to: 'ReplyTo | None', message_id: str | None):
        self.body = body
        self.properties = properties
        self.reply_to = reply_to
        self.message_id = message_id
        self._ack_fn = ack_fn

    def acknowledge(self):
        """Acknowledge this message. Required when the session uses CLIENT_ACK mode."""
        self._ack_fn()


_RECEIVE_TIMEOUT_MS = 200  # polling interval used by __next__ to allow signal delivery

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

        text_ptr = c_char_p()
        ems_lib.tibemsTextMsg_GetText(message, byref(text_ptr))
        body = text_ptr.value.decode('utf-8') if text_ptr.value else ""
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

        return ReceivedMessage(body=body, properties=properties, ack_fn=ack_fn, reply_to=reply_to, message_id=message_id)

    def _destroy_current(self):
        if self._current_msg_handle is not None:
            ems_lib.tibemsMsg_Destroy(self._current_msg_handle)
            self._current_msg_handle = None


def create_consumer(session, destination, ack_mode: AckMode, selector: str = None, no_local: bool = False) -> TibEMSConsumer:
    return TibEMSConsumer(session, destination, ack_mode, selector, no_local)


# Callback type for tibemsMsgConsumer_SetMsgListener:
# void fn(tibemsMsgConsumer consumer, tibemsMsg message, void* closure)
_MsgListenerCallback = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)

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
            message = c_void_p(message)
            # Extract all data now — the handle may be invalid after this returns.
            text_ptr = c_char_p()
            ems_lib.tibemsTextMsg_GetText(message, byref(text_ptr))
            body = text_ptr.value.decode('utf-8') if text_ptr.value else ""

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
                body=body, properties=properties,
                ack_fn=ack_fn, reply_to=reply_to, message_id=message_id
            )
            self._loop.call_soon_threadsafe(self._msg_queue.put_nowait, received)

        self._callback = _MsgListenerCallback(_on_message)
        ems_lib.tibemsMsgConsumer_SetMsgListener(self._consumer, self._callback, None)
        return self

    async def __aexit__(self, *_):
        if self._consumer is not None:
            ems_lib.tibemsMsgConsumer_Close(self._consumer)
            self._consumer = None

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

def create_message(message_text: str, jms_props: list[JMS_Property]=[], correlation_id: str | None = None):
    message = c_void_p()
    ems_lib.tibemsTextMsg_Create(byref(message))
    ems_lib.tibemsTextMsg_SetText(message, bytes(message_text, "utf-8"))
    if correlation_id is not None:
        res = ems_lib.tibemsMsg_SetCorrelationID(message, bytes(correlation_id, "utf-8"))
        if res != TIBEMS_OK:
            raise TibEMSSetHeaderError(res, "JMSCorrelationID")
    for jms_prop in jms_props:
        match jms_prop.type:
            case JmsPropertyType.String:
                ems_lib.tibemsMsg_SetStringProperty(message, bytes(jms_prop.name, "utf-8"), bytes(jms_prop.value, "utf-8"))
            case JmsPropertyType.Boolean:
                ems_lib.tibemsMsg_SetBooleanProperty(message, bytes(jms_prop.name, "utf-8"), c_bool(jms_prop.value))
            case JmsPropertyType.Byte:
                ems_lib.tibemsMsg_SetByteProperty(message, bytes(jms_prop.name, "utf-8"), c_byte(jms_prop.value))
            case JmsPropertyType.Short:
                ems_lib.tibemsMsg_SetShortProperty(message, bytes(jms_prop.name, "utf-8"), c_short(jms_prop.value))
            case JmsPropertyType.Integer:
                ems_lib.tibemsMsg_SetIntProperty(message, bytes(jms_prop.name, "utf-8"), c_int(jms_prop.value))
            case JmsPropertyType.Long:
                ems_lib.tibemsMsg_SetLongProperty(message, bytes(jms_prop.name, "utf-8"), c_long(jms_prop.value))
            case JmsPropertyType.Float:
                ems_lib.tibemsMsg_SetFloatProperty(message, bytes(jms_prop.name, "utf-8"), c_float(jms_prop.value))
            case JmsPropertyType.Double:
                ems_lib.tibemsMsg_SetDoubleProperty(message, bytes(jms_prop.name, "utf-8"), c_double(jms_prop.value))
    return message

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


def destroy_message(message):
    ems_lib.tibemsMsg_Destroy(message)

def get_ems_message(status_code):
    """Resolves an EMS status code to a text message."""
    message_bytes = ems_lib.tibemsStatus_GetText(status_code)
    
    if message_bytes:
        # Decode the byte-string returned by C to a Python string
        return message_bytes.decode('utf-8')
    return "Unknown Status Code"

def queue_publish(ems_url: str, ems_username: str, ems_password: str, queue_name: str, message_txt: str, jms_props: list[JmsPropertyType]=[]) -> str:
    with tibems_connection(url=ems_url, username=ems_username, password=ems_password) as connection:
        with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
            queue = create_destination(name=queue_name, type=DestinationType.Queue)
            producer = create_producer(session, queue)
            with tibems_message(message_text=message_txt, jms_props=jms_props) as message:
                message_id, _ = publish_message(producer, message=message)
    return message_id

def topic_publish(ems_url: str, ems_username: str, ems_password: str, topic_name: str, message_txt: str, jms_props: list[JMS_Property]=[]) -> str:
    with tibems_connection(url=ems_url, username=ems_username, password=ems_password) as connection:
        with tibems_session(connection=connection, transacted=False, ack_mode=AckMode.TIBEMS_AUTO_ACK) as session:
            topic = create_destination(name=topic_name, type=DestinationType.Topic)
            producer = create_producer(session, topic)
            with tibems_message(message_text=message_txt, jms_props=jms_props) as message:
                message_id, _ = publish_message(producer, message=message)
    return message_id

class TibEMSConnectionError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error connecting to Tibco EMS: {status} - {msg}")

class TibEMSSessionError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error creating a Tibco EMS session: {status} - {msg}")

class TibEMSCreateDestinationError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error creating a Tibco EMS destination: {status} - {msg}")

class TibEMSCreateProducerError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error creating a Tibco EMS producer: {status} - {msg}")

class TibEMSCreateConsumerError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error creating a Tibco EMS consumer: {status} - {msg}")

class TibEMSSetHeaderError(Exception):
    def __init__(self, status, header_name: str):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error setting JMS header '{header_name}': {status} - {msg}")

class TibEMSPublishError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error publishing to Tibco EMS destination: {status} - {msg}")

class TibEMSConfigurationError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(msg)