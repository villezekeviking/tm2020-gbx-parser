"""Binary reader utilities for GBX files with little-endian support."""

import struct


def read_uint8(f):
    """Read unsigned 8-bit integer."""
    data = f.read(1)
    if len(data) != 1:
        raise EOFError("Failed to read uint8")
    return struct.unpack('<B', data)[0]


def read_uint16(f):
    """Read unsigned 16-bit integer (little-endian)."""
    data = f.read(2)
    if len(data) != 2:
        raise EOFError("Failed to read uint16")
    return struct.unpack('<H', data)[0]


def read_int16(f):
    """Read signed 16-bit integer (little-endian)."""
    data = f.read(2)
    if len(data) != 2:
        raise EOFError("Failed to read int16")
    return struct.unpack('<h', data)[0]


def read_int32(f):
    """Read signed 32-bit integer (little-endian)."""
    data = f.read(4)
    if len(data) != 4:
        raise EOFError("Failed to read int32")
    return struct.unpack('<i', data)[0]


def read_uint32(f):
    """Read unsigned 32-bit integer (little-endian)."""
    data = f.read(4)
    if len(data) != 4:
        raise EOFError("Failed to read uint32")
    return struct.unpack('<I', data)[0]


def read_float(f):
    """Read 32-bit float (little-endian)."""
    data = f.read(4)
    if len(data) != 4:
        raise EOFError("Failed to read float")
    return struct.unpack('<f', data)[0]


def read_vec3(f):
    """Read 3D vector (3 floats)."""
    x = read_float(f)
    y = read_float(f)
    z = read_float(f)
    return (x, y, z)


def read_string(f):
    """Read length-prefixed string (basic version, no lookback)."""
    length = read_uint32(f)
    if length == 0:
        return ""
    if length > 100000:  # Sanity check
        return ""
    data = f.read(length)
    if len(data) != length:
        raise EOFError(f"Failed to read string of length {length}")
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        return data.decode('latin-1', errors='ignore')


def read_data(f):
    """Read length-prefixed byte array (MwBuffer/Data)."""
    length = read_uint32(f)
    if length == 0:
        return b''
    if length > 100000000:  # Sanity check: 100MB
        raise ValueError(f"Unreasonable data length: {length}")
    data = f.read(length)
    if len(data) != length:
        raise EOFError(f"Failed to read data of length {length}")
    return data
