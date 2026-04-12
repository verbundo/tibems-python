# Author: Vadym Korol <vadym.korol@verbundo.com>
# License: MIT (see LICENSE in the project root)
#
# Message-related helpers: JmsPropertyType, JMS_Property, tibems_message
# context manager, create_message, and destroy_message.

from contextlib import contextmanager
from ctypes import c_char_p, c_void_p, byref, c_bool, c_float, c_byte, c_short, c_double, c_long, c_int
from enum import Enum

from .tibems import ems_lib, TIBEMS_OK, TibEMSSetHeaderError, TibEMSCreateMessageError, TibEMSConfigurationError

ems_lib.tibemsSession_CreateBytesMessage.argtypes = [c_void_p, c_void_p]
ems_lib.tibemsSession_CreateBytesMessage.restype  = c_int

ems_lib.tibemsBytesMsg_WriteBytes.argtypes = [c_void_p, c_char_p, c_int, c_int]
ems_lib.tibemsBytesMsg_WriteBytes.restype  = c_int


class JmsPropertyType(int, Enum):
    String=0
    Boolean=1
    Byte=2
    Short=3
    Integer=4
    Long=5
    Float=6
    Double=7


class JMS_Property:
    def __init__(self, name: str, value: str|bool|int|float, type: JmsPropertyType=JmsPropertyType.String):
        self.name = name
        self.value = value
        self.type = type


def create_message(message_body: str | bytes, jms_props: list[JMS_Property] | None = None, correlation_id: str | None = None, session=None):
    if jms_props is None:
        jms_props = []
    message = c_void_p()
    if isinstance(message_body, bytes):
        if session is None:
            raise TibEMSConfigurationError("'session' is required when message_body is bytes")
        res = ems_lib.tibemsSession_CreateBytesMessage(session, byref(message))
        if res != TIBEMS_OK:
            raise TibEMSCreateMessageError(res)
        res = ems_lib.tibemsBytesMsg_WriteBytes(message, message_body, 0, len(message_body))
        if res != TIBEMS_OK:
            raise TibEMSCreateMessageError(res)
    else:
        ems_lib.tibemsTextMsg_Create(byref(message))
        ems_lib.tibemsTextMsg_SetText(message, message_body.encode("utf-8"))
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


def destroy_message(message):
    ems_lib.tibemsMsg_Destroy(message)


@contextmanager
def tibems_message(message_body: str | bytes, jms_props: list[JMS_Property] | None = None, correlation_id: str | None = None, session=None):
    if jms_props is None:
        jms_props = []
    message = create_message(message_body=message_body, jms_props=jms_props, correlation_id=correlation_id, session=session)
    try:
        yield message
    finally:
        destroy_message(message=message)
