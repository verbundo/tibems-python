# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Python wrapper for the TIBCO Enterprise Message Service (EMS) C API.
# Uses ctypes to call into the native libtibems.so shared library, exposing
# Pythonic context managers for connections, sessions, messages, producers,
# and consumers.

from contextlib import contextmanager
import ctypes
import ctypes.util
from ctypes import CFUNCTYPE, c_char_p, c_void_p, byref, POINTER, c_int
from enum import Enum
import sys
from typing import Callable, Generator


# ---------------------------------------------------------------------------
# Load library
# ---------------------------------------------------------------------------

def _load_library():
    if sys.platform == "win32":
        name = "tibems"
    elif sys.platform == "darwin":
        name = "tibemsC"
    else:
        name = "tibems"
    path = ctypes.util.find_library(name)
    if path is None:
        raise RuntimeError(
            f"EMS library '{name}' not found. "
            "Add <EMS_HOME>/bin or <EMS_HOME>/lib to your library path."
        )
    return ctypes.CDLL(path)

ems_lib = _load_library()


TIBEMS_OK = 0
TIBEMS_FALSE = 0
TIBEMS_TRUE = 1

# C callback type for tibemsConnection_SetExceptionListener:
# void fn(tibemsConnection conn, tibemsMsg errorMsg, void* closure)
_ExceptionListenerCallback = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)

__all__ = [
    "ems_lib",
    "TIBEMS_OK",
    "TIBEMS_FALSE",
    "TIBEMS_TRUE",
    "TIBEMS_SSL_ENCODING_AUTO",
    "tibems_status",
    "tibems_bool",
    "AckMode",
    "DestinationType",
    "ProtocolType",
    "tibems_connection",
    "tibems_session",
    "create_destination",
    "ReplyTo",
    "ReceivedMessage",
    "get_ems_message",
    "TibEMSConnectionError",
    "TibEMSSessionError",
    "TibEMSCreateDestinationError",
    "TibEMSCreateProducerError",
    "TibEMSCreateConsumerError",
    "TibEMSCreateMessageError",
    "TibEMSSetHeaderError",
    "TibEMSPublishError",
    "TibEMSConfigurationError",
]

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

ems_lib.tibemsSession_Commit.argtypes = [c_void_p]
ems_lib.tibemsSession_Rollback.argtypes = [c_void_p]

class DestinationType(int, Enum):
    Queue=0
    Topic=1

class ProtocolType(str, Enum):
    TCP="tcp"
    SSL="ssl"


@contextmanager
def tibems_connection(
    url: str,
    username: str,
    password: str,
    start_connection: bool = False,
    server_cert: str | None = None,
    client_cert: str | None = None,
    private_key: str | None = None,
    verify_server_cert: bool = True,
    client_id: str | None = None,
    exception_listener: Callable[[str], None] | None = None,
) -> "Generator[c_void_p, None, None]":
    """Open a connection to the TIBCO EMS server.

    Args:
        url: Server URL (e.g. ``tcp://localhost:7222`` or ``ssl://...``).
        username: JMS username.
        password: JMS password.
        start_connection: Whether to call ``tibemsConnection_Start`` immediately.
        server_cert: Path to the server's trusted certificate (required for SSL
            when ``verify_server_cert`` is ``True``).
        client_cert: Path to the client certificate in PKCS#12 format.
        private_key: Unused placeholder — the C API expects the key inside the
            PKCS#12 file referenced by ``client_cert``.
        verify_server_cert: Whether to verify the server's certificate chain.
        client_id: Optional JMS client identifier. Required when using durable
            topic subscriptions — EMS uses this ID (combined with the
            subscription name) to persist subscription state across reconnects.
        exception_listener: Optional callable invoked by EMS on connection
            events: loss, FT reconnect, or cluster failover. Receives a single
            ``str`` describing the event. Called on an EMS internal thread —
            keep the implementation short and thread-safe.

    Yields:
        A ``c_void_p`` handle to the opened connection.

    Raises:
        TibEMSConfigurationError: If SSL is required but ``server_cert`` is missing.
        TibEMSConnectionError: If the connection cannot be established.
    """
    ems_lib.tibems_Open()
    use_ssl: bool = url.startswith("ssl:")
    ssl_params: int | None = None
    factory: int | None = None
    connection_opened = False

    try:
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
        if factory is None:
            raise TibEMSConnectionError(-1)

        if ssl_params:
            ems_lib.tibemsConnectionFactory_SetSSLParams(factory, ssl_params)
        ems_lib.tibemsConnectionFactory_SetServerURL(factory, bytes(url, "utf-8"))

        connection = c_void_p()
        res = ems_lib.tibemsConnectionFactory_CreateConnection(factory, byref(connection), bytes(username, "utf-8"), bytes(password, "utf-8"))
        if res != TIBEMS_OK:
            raise TibEMSConnectionError(res)
        connection_opened = True

        if client_id:
            ems_lib.tibemsConnection_SetClientID(connection, bytes(client_id, "utf-8"))

        _exception_cb = None  # kept alive for the duration of the connection
        if exception_listener is not None:
            def _c_exception_cb(conn, error_msg, closure):
                try:
                    text_ptr = c_char_p()
                    ems_lib.tibemsTextMsg_GetText(c_void_p(error_msg), byref(text_ptr))
                    text = text_ptr.value.decode("utf-8") if text_ptr.value else "Connection exception"
                    exception_listener(text)
                except Exception:
                    pass
            _exception_cb = _ExceptionListenerCallback(_c_exception_cb)
            ems_lib.tibemsConnection_SetExceptionListener(connection, _exception_cb, None)

        if start_connection:
            ems_lib.tibemsConnection_Start(connection)

        yield connection
    finally:
        if connection_opened:
            ems_lib.tibemsConnection_Close(connection)
        if ssl_params is not None:
            ems_lib.tibemsSSLParams_Destroy(ssl_params)
        if factory is not None:
            ems_lib.tibemsConnectionFactory_Destroy(factory)
        if connection_opened:
            ems_lib.tibems_Close()

@contextmanager
def tibems_session(connection: c_void_p, transacted: bool = False, ack_mode: AckMode = AckMode.TIBEMS_AUTO_ACK) -> Generator[c_void_p, None, None]:
    """Create a JMS session on an existing connection.

    Args:
        connection: A ``c_void_p`` handle from :func:`tibems_connection`.
        transacted: Whether the session should be transacted.
        ack_mode: The acknowledgment mode (``AUTO_ACK``, ``CLIENT_ACK``, or
            ``DUPS_OK_ACK``).

    Yields:
        A ``c_void_p`` handle to the created session.

    Raises:
        TibEMSSessionError: If the session cannot be created.
    """
    session = c_void_p()
    res = ems_lib.tibemsConnection_CreateSession(connection, byref(session), transacted, ack_mode.value)
    if res != TIBEMS_OK:
        raise TibEMSSessionError(res)
    try:
        yield session
    finally:
        ems_lib.tibemsSession_Close(session)

def session_commit(session_handle: c_void_p):
    """Commit a transacted session."""
    res = ems_lib.tibemsSession_Commit(session_handle)
    if res != TIBEMS_OK:
        raise TibEMSSessionError(res)

def session_rollback(session_handle: c_void_p):
    """Rollback a transacted session."""
    res = ems_lib.tibemsSession_Rollback(session_handle)
    if res != TIBEMS_OK:
        raise TibEMSSessionError(res)

def create_destination(name: str, dest_type: DestinationType = DestinationType.Queue) -> c_void_p:
    """Create a JMS destination (queue or topic).

    Args:
        name: The destination name.
        dest_type: Either ``DestinationType.Queue`` or ``DestinationType.Topic``.

    Returns:
        A ``c_void_p`` handle to the created destination.

    Raises:
        TibEMSCreateDestinationError: If the destination cannot be created.
    """
    destination = c_void_p()
    if dest_type == DestinationType.Queue:
        res = ems_lib.tibemsQueue_Create(byref(destination), bytes(name, "utf-8"))
    else:
        res = ems_lib.tibemsTopic_Create(byref(destination), bytes(name, "utf-8"))
    if res != TIBEMS_OK:
        raise TibEMSCreateDestinationError(res)
    return destination

class ReplyTo:
    """JMSReplyTo destination from a received message.
    The handle is valid until the next message is received from the consumer."""
    def __init__(self, name: str, handle: c_void_p):
        self.name = name
        self.handle = handle  # usable directly as a destination in create_producer()


class ReceivedMessage:
    def __init__(self, body: str, properties: list[dict], ack_fn, reply_to: 'ReplyTo | None', message_id: str | None, body_bytes: bytes | None = None):
        self.body = body
        self.body_bytes = body_bytes
        self.properties = properties
        self.reply_to = reply_to
        self.message_id = message_id
        self._ack_fn = ack_fn

    def acknowledge(self):
        """Acknowledge this message. Required when the session uses CLIENT_ACK mode."""
        self._ack_fn()


def get_ems_message(status_code):
    """Resolves an EMS status code to a text message."""
    message_bytes = ems_lib.tibemsStatus_GetText(status_code)
    
    if message_bytes:
        # Decode the byte-string returned by C to a Python string
        return message_bytes.decode('utf-8')
    return "Unknown Status Code"

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

class TibEMSCreateMessageError(Exception):
    def __init__(self, status):
        self.status = status
        msg = get_ems_message(status)
        super().__init__(f"Error creating a Tibco EMS message: {status} - {msg}")

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