"""Binary reader utilities for GBX files."""

import struct
from typing import BinaryIO, Optional


class GBXReader:
    """Binary reader for GBX files with little-endian support."""
    
    def __init__(self, file_obj: BinaryIO):
        """Initialize reader with binary file object."""
        self.file = file_obj
        self.lookback_strings = []  # For GBX string optimization
        
    def tell(self) -> int:
        """Get current position in file."""
        return self.file.tell()
        
    def seek(self, position: int):
        """Seek to position in file."""
        self.file.seek(position)
        
    def read_bytes(self, count: int) -> bytes:
        """Read raw bytes."""
        data = self.file.read(count)
        if len(data) != count:
            raise EOFError(f"Expected {count} bytes, got {len(data)}")
        return data
        
    def read_uint8(self) -> int:
        """Read unsigned 8-bit integer."""
        return struct.unpack('<B', self.read_bytes(1))[0]
        
    def read_uint16(self) -> int:
        """Read unsigned 16-bit integer (little-endian)."""
        return struct.unpack('<H', self.read_bytes(2))[0]
        
    def read_uint32(self) -> int:
        """Read unsigned 32-bit integer (little-endian)."""
        return struct.unpack('<I', self.read_bytes(4))[0]
        
    def read_int32(self) -> int:
        """Read signed 32-bit integer (little-endian)."""
        return struct.unpack('<i', self.read_bytes(4))[0]
        
    def read_float(self) -> float:
        """Read 32-bit float (little-endian)."""
        return struct.unpack('<f', self.read_bytes(4))[0]
        
    def read_string(self) -> str:
        """Read length-prefixed string with GBX lookback support."""
        length = self.read_uint32()
        
        # Handle lookback strings (GBX optimization)
        if length == 0:
            return ""
        elif length == 0x80000000 or length >= 0xC0000000:
            # Lookback string reference
            if length == 0x80000000:
                # Empty lookback
                return ""
            else:
                # Reference to previous string
                index = length & 0x3FFF
                if index == 0 or index > len(self.lookback_strings):
                    return ""
                return self.lookback_strings[index - 1]
        else:
            # Normal string
            if length > 0x10000:  # Sanity check
                return ""
            string_bytes = self.read_bytes(length)
            try:
                string = string_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # Try latin-1 as fallback
                string = string_bytes.decode('latin-1', errors='ignore')
            
            # Add to lookback strings
            self.lookback_strings.append(string)
            if len(self.lookback_strings) > 0x1000:  # Limit lookback size
                self.lookback_strings.pop(0)
                
            return string
            
    def read_vec3(self) -> tuple[float, float, float]:
        """Read 3D vector (3 floats)."""
        x = self.read_float()
        y = self.read_float()
        z = self.read_float()
        return (x, y, z)
