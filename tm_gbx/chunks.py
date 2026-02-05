"""Chunk parsers for GBX files."""

import struct
from typing import Dict, Any
from .reader import GBXReader


class ChunkParser:
    """Parser for GBX chunks."""
    
    @staticmethod
    def parse_chunk_0x03093000(reader: GBXReader, size: int) -> Dict[str, Any]:
        """Parse chunk 0x03093000 - Map info, time, player nickname/login."""
        start_pos = reader.tell()
        data = {}
        
        try:
            # Skip version
            _ = reader.read_uint32()
            
            # Read map info
            map_uid = reader.read_string()
            map_name = reader.read_string()
            
            data['map_uid'] = map_uid
            data['map_name'] = map_name
            
            # Skip some bytes to get to player info
            # The exact structure varies, so we'll try to read what we can
            
        except Exception as e:
            # If we fail, just skip to end of chunk
            pass
            
        # Ensure we're at the end of the chunk
        end_pos = start_pos + size
        if reader.tell() < end_pos:
            reader.seek(end_pos)
            
        return data
        
    @staticmethod
    def parse_chunk_0x03093002(reader: GBXReader, size: int) -> Dict[str, Any]:
        """Parse chunk 0x03093002 - Author info."""
        start_pos = reader.tell()
        data = {}
        
        try:
            # Skip version
            _ = reader.read_uint32()
            
            # Read author login
            author_login = reader.read_string()
            data['map_author'] = author_login
            
        except Exception as e:
            pass
            
        # Ensure we're at the end of the chunk
        end_pos = start_pos + size
        if reader.tell() < end_pos:
            reader.seek(end_pos)
            
        return data
        
    @staticmethod
    def parse_chunk_0x03093018(reader: GBXReader, size: int) -> Dict[str, Any]:
        """Parse chunk 0x03093018 - Title ID and author."""
        start_pos = reader.tell()
        data = {}
        
        try:
            # Skip version
            _ = reader.read_uint32()
            
            # Read title ID
            title_id = reader.read_string()
            data['title_id'] = title_id
            
            # Try to read author
            author = reader.read_string()
            if author:
                data['map_author'] = author
                
        except Exception as e:
            pass
            
        # Ensure we're at the end of the chunk
        end_pos = start_pos + size
        if reader.tell() < end_pos:
            reader.seek(end_pos)
            
        return data
