"""GBX header parser.

Based on GbxHeaderBasic.Parse() and GbxHeaderReader from gbx-net.
"""

import re
from .reader import read_uint8, read_uint16, read_uint32, read_int32, read_string
from .lookback import LookbackReader


def parse_header(f):
    """Parse GBX header and return metadata dictionary.
    
    Args:
        f: Binary file object positioned at start of file
        
    Returns:
        Dictionary with parsed metadata
    """
    # Read magic bytes
    magic = f.read(3)
    if magic != b'GBX':
        raise ValueError(f"Invalid GBX file: magic bytes are {magic.hex()}")
    
    # Read version
    version = read_uint16(f)
    
    # Read format
    format_byte = read_uint8(f)
    
    # Read compression info
    ref_table_compressed = read_uint8(f)
    body_compressed = read_uint8(f)
    
    # If version >= 4: read unknown byte
    if version >= 4:
        unknown_byte = read_uint8(f)
    
    # Read class_id
    class_id = read_uint32(f)
    
    # For CGameCtnReplayRecord
    if class_id != 0x03093000:
        # Not a replay record - continue anyway but warn
        pass
    
    # Read user_data_size
    user_data_size = read_uint32(f)
    
    metadata = {}
    
    # Parse header chunks if user_data_size > 0
    if user_data_size > 0:
        # Remember start of user data section
        user_data_start = f.tell()
        
        num_header_chunks = read_uint32(f)
        
        # Read all chunk headers first
        chunk_headers = []
        for _ in range(num_header_chunks):
            chunk_id = read_uint32(f)
            chunk_size_raw = read_int32(f)
            
            # High bit is isHeavy flag
            is_heavy = (chunk_size_raw & 0x80000000) != 0
            chunk_size = chunk_size_raw & 0x7FFFFFFF
            
            chunk_headers.append({
                'id': chunk_id,
                'size': chunk_size,
                'is_heavy': is_heavy
            })
        
        # Now parse chunk data
        lookback = LookbackReader()
        
        for chunk in chunk_headers:
            chunk_id = chunk['id']
            chunk_size = chunk['size']
            
            # Save position before chunk data
            chunk_start = f.tell()
            
            # Parse known chunks
            if chunk_id == 0x03093000:
                # HeaderChunk03093000
                chunk_version = read_uint32(f)
                
                if chunk_version >= 4 and chunk_version != 9999:
                    # MapInfo via read_ident
                    map_info = lookback.read_ident(f)
                    if map_info[0]:  # id is the map UID
                        metadata['map_uid'] = map_info[0]
                    if map_info[2]:  # author
                        metadata['map_author'] = map_info[2]
                
                # Time as int32 (nullable, -1 = None)
                time = read_int32(f)
                if time >= 0:
                    metadata['race_time_ms'] = time
                
                # PlayerNickname
                nickname = read_string(f)
                if nickname:
                    metadata['player_nickname'] = nickname
                
                # If version >= 6: PlayerLogin
                if chunk_version >= 6:
                    login = read_string(f)
                    if login:
                        metadata['player_login'] = login
                
                # If version > 7: skip 1 byte, read TitleId
                if chunk_version > 7:
                    read_uint8(f)  # Skip 1 byte
                    title_id = lookback.read_id(f)
                    if title_id:
                        metadata['title_id'] = title_id
            
            elif chunk_id == 0x03093001:
                # HeaderChunk03093001: XML string
                xml_string = read_string(f)
                if xml_string:
                    metadata['xml_data'] = xml_string
                    
                    # Try to extract map_name from XML
                    if 'map name="' in xml_string:
                        match = re.search(r'map name="([^"]+)"', xml_string)
                        if match:
                            metadata['map_name'] = match.group(1)
                    
                    # Try to extract race time from XML if not already set
                    if 'race_time_ms' not in metadata and 'times best="' in xml_string:
                        match = re.search(r'times best="(\d+)"', xml_string)
                        if match:
                            metadata['race_time_ms'] = int(match.group(1))
                    
                    # Try to extract checkpoint count
                    if 'checkpoints cur="' in xml_string:
                        match = re.search(r'checkpoints cur="(\d+)"', xml_string)
                        if match:
                            metadata['num_checkpoints'] = int(match.group(1))
            
            elif chunk_id == 0x03093002:
                # HeaderChunk03093002: Author info
                chunk_version = read_int32(f)
                author_version = read_int32(f)
                author_login = read_string(f)
                author_nickname = read_string(f)
                author_zone = read_string(f)
                author_extra = read_string(f)
                
                if author_login:
                    metadata['author_login'] = author_login
                if author_nickname:
                    metadata['author_nickname'] = author_nickname
            
            # Skip to end of chunk
            f.seek(chunk_start + chunk_size)
        
        # Make sure we're at the end of user_data section
        f.seek(user_data_start + user_data_size)
    
    # Read num_nodes
    num_nodes = read_int32(f)
    
    return {
        'version': version,
        'format': format_byte,
        'ref_table_compressed': ref_table_compressed,
        'body_compressed': body_compressed,
        'class_id': class_id,
        'user_data_size': user_data_size,
        'num_nodes': num_nodes,
        'metadata': metadata
    }
