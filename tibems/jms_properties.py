import ctypes
from ctypes import c_void_p, c_int, c_char_p, byref, POINTER, Structure, Union, c_byte, c_longlong, c_float, c_double, c_ushort

# EMS Status Codes [4, 5]
TIBEMS_OK = 0
TIBEMS_NOT_FOUND = 35

# Type Indicators from TIBCO EMS C API [2, 3]
# Format: {TypeID: (DisplayLabel, UnionAttributeName)}
TYPE_MAP = {
    0x01: ("BOOLEAN", "boolValue"),
    0x02: ("BYTE", "byteValue"),
    0x04: ("SHORT", "shortValue"),
    0x05: ("INTEGER", "intValue"),
    0x06: ("LONG", "longValue"),
    0x07: ("FLOAT", "floatValue"),
    0x08: ("DOUBLE", "doubleValue"),
    0x09: ("STRING", "utf8Value"),
    0x0A: ("BYTES", "bytesValue"),
}

# Define C-compatible structures based on the source definitions [3, 6]
class tibemsData(Union):
    _fields_ = [
        ("boolValue", ctypes.c_bool),
        ("byteValue", ctypes.c_byte),
        ("shortValue", ctypes.c_short),
        ("wcharValue", c_ushort),
        ("intValue", ctypes.c_int),
        ("longValue", c_longlong),
        ("floatValue", c_float),
        ("doubleValue", c_double),
        ("utf8Value", c_char_p),
        ("bytesValue", c_void_p),
        ("msgValue", c_void_p),
        ("arrayValue", c_void_p),
    ]

class tibemsMsgField(Structure):
    _fields_ = [
        ("type", c_byte),
        ("size", c_int),
        ("count", c_int),
        ("data", tibemsData),
    ]

def get_standard_jms_properties(ems_lib, message_handle):
    """Retrieves JMS standard header fields via dedicated C API getters.
    These are NOT returned by tibemsMsg_GetPropertyNames."""
    results = []

    for name, func_name in [
        ("JMSMessageID",     "tibemsMsg_GetMessageID"),
        ("JMSCorrelationID", "tibemsMsg_GetCorrelationID"),
        ("JMSType",          "tibemsMsg_GetType"),
    ]:
        ptr = c_char_p()
        if getattr(ems_lib, func_name)(message_handle, byref(ptr)) == TIBEMS_OK and ptr.value:
            results.append({"name": name, "type": "STRING", "value": ptr.value.decode('utf-8')})

    for name, func_name in [
        ("JMSTimestamp",  "tibemsMsg_GetTimestamp"),
        ("JMSExpiration", "tibemsMsg_GetExpiration"),
    ]:
        val = c_longlong(0)
        if getattr(ems_lib, func_name)(message_handle, byref(val)) == TIBEMS_OK:
            results.append({"name": name, "type": "LONG", "value": val.value})

    for name, func_name in [
        ("JMSDeliveryMode", "tibemsMsg_GetDeliveryMode"),
        ("JMSPriority",     "tibemsMsg_GetPriority"),
    ]:
        val = c_int(0)
        if getattr(ems_lib, func_name)(message_handle, byref(val)) == TIBEMS_OK:
            results.append({"name": name, "type": "INTEGER", "value": val.value})

    redelivered = c_int(0)
    if ems_lib.tibemsMsg_GetRedelivered(message_handle, byref(redelivered)) == TIBEMS_OK:
        results.append({"name": "JMSRedelivered", "type": "BOOLEAN", "value": bool(redelivered.value)})

    return results


def get_properties_with_types(ems_lib, message_handle):
    results = []
    enum_handle = c_void_p()
    
    # 1. Initialize the enumeration of property names [7, 8]
    status = ems_lib.tibemsMsg_GetPropertyNames(message_handle, byref(enum_handle))
    if status != TIBEMS_OK:
        return results

    try:
        while True:
            prop_name_ptr = c_char_p()
            # 2. Get the next property name [3, 9]
            status = ems_lib.tibemsMsgEnum_GetNextName(enum_handle, byref(prop_name_ptr))
            
            if status == TIBEMS_NOT_FOUND: # End of enumeration [3]
                break
                
            if status == TIBEMS_OK and prop_name_ptr.value:
                name = prop_name_ptr.value
                field = tibemsMsgField()
                
                # 3. Fetch the full property field structure [3, 10]
                v_status = ems_lib.tibemsMsg_GetProperty(message_handle, name, byref(field))
                
                if v_status == TIBEMS_OK:
                    # FIX: Safely retrieve the type info tuple
                    type_info = TYPE_MAP.get(field.type, ("UNKNOWN", None))
                    
                    # FIX: Access tuple indices 0 and 1 correctly
                    type_label = type_info[0]  # e.g., "STRING"
                    attr_name = type_info[1]   # e.g., "utf8Value"
                    
                    # Extract raw value from the union using the attribute name [6]
                    raw_val = getattr(field.data, attr_name) if attr_name else "N/A"
                    
                    # Decode string bytes for cleaner output
                    if isinstance(raw_val, bytes):
                        raw_val = raw_val.decode('utf-8')
                        
                    results.append({
                        "name": name.decode(),
                        "type": type_label,
                        "value": raw_val
                    })

    finally:
        # 4. Reclaim memory by destroying the enumerator [12]
        ems_lib.tibemsMsgEnum_Destroy(enum_handle)

    results.extend(get_standard_jms_properties(ems_lib, message_handle))
    return results