"""Lookback string/ID reader for GBX string interning system.

Based on GbxReader.ReadId(), ReadIdAsString(), ReadIdIndex(), and ReadIdent() 
from gbx-net reference implementation.
"""

from .reader import read_uint32, read_string


class LookbackReader:
    """Manages lookback string reading (GBX's string interning system)."""
    
    def __init__(self):
        self.id_version = None
        self.lookback_strings = {}
        self.counter = 0
    
    def reset(self):
        """Reset lookback state (for body parsing)."""
        self.lookback_strings = {}
        self.counter = 0
    
    def read_id(self, f):
        """Read an ID (lookback string)."""
        # First call reads IdVersion
        if self.id_version is None:
            self.id_version = read_uint32(f)
            if self.id_version < 3:
                raise ValueError(f"Unsupported ID version: {self.id_version}")
        
        # Read index
        index = read_uint32(f)
        
        # Handle special cases
        if index == 0xFFFFFFFF:
            return ""
        
        # Check bits 30-31
        high_bits = (index >> 30) & 0x3
        
        # If bits 30-31 are NOT 01 or 10, it's a collection number
        if high_bits != 1 and high_bits != 2:
            # It's an unassigned/collection number - treat as empty
            return ""
        
        # Check if it's a reference to existing string
        masked_index = index & 0x3FFFFFFF
        if masked_index != 0:
            # Reuse previously stored string
            if masked_index in self.lookback_strings:
                return self.lookback_strings[masked_index]
            else:
                # Not found - return empty
                return ""
        
        # Otherwise read a new string
        string = read_string(f)
        
        # Store it with key = index + counter + 1
        key = index + self.counter + 1
        self.lookback_strings[key] = string
        self.counter += 1
        
        return string
    
    def read_ident(self, f):
        """Read an Ident (3 IDs: id, collection, author)."""
        id_str = self.read_id(f)
        collection = self.read_id(f)
        author = self.read_id(f)
        return (id_str, collection, author)
