"""Main GBX parser entry point.

Pure-Python parser for TrackMania 2020 replay files (.Gbx).
"""

import zlib
from .header import parse_header
from .ghost import parse_ghost_from_body
from .reader import read_int32, read_uint32, read_string


def parse_gbx(filepath):
    """Parse a GBX replay file.
    
    Args:
        filepath: Path to .Gbx replay file
        
    Returns:
        Dictionary with 'metadata', 'ghost_info', and 'ghost_samples' keys
    """
    with open(filepath, 'rb') as f:
        # Parse header
        header_data = parse_header(f)
        metadata = header_data.get('metadata', {})
        
        # Skip ref table
        num_external = read_int32(f)
        
        if num_external > 0:
            # Read external refs (most TM2020 replays have 0)
            for _ in range(num_external):
                # Skip external node info
                # flags (int32), file path (string), or node_index (int32)
                flags = read_int32(f)
                if (flags & 0x4) != 0:
                    # Has file path
                    file_path = read_string(f)
                else:
                    # Has node index instead of file path
                    file_node_index = read_int32(f)
                # Use resource index if needed
                if (flags & 0x8) != 0:
                    resource_index = read_int32(f)
                # Node index
                node_index = read_int32(f)
                # Use flags
                use_flags = read_int32(f)
                # Folder deps
                if (flags & 0x10) != 0:
                    folder_dep_count = read_int32(f)
        
        # Read body - handle both zlib (.Ghost.Gbx) and LZO (replay .Gbx) compression
        body_data = None
        ghost_info = None
        ghost_samples = []
        
        body_compressed = header_data.get('body_compressed', 0)
        
        if body_compressed == 0x43:  # 'C' = compressed
            # Read uncompressed_size and compressed_size
            uncompressed_size = read_uint32(f)
            compressed_size = read_uint32(f)
            
            # Read compressed data
            compressed_data = f.read(compressed_size)
            
            # Try zlib first (for .Ghost.Gbx files)
            try:
                body_data = zlib.decompress(compressed_data)
            except zlib.error:
                # Fall back to LZO (for replay .Gbx files) if available
                try:
                    import lzo
                    body_data = lzo.decompress(compressed_data, False, uncompressed_size)
                except ImportError:
                    # LZO not available - can't decompress replay body
                    body_data = None
                except Exception:
                    body_data = None
        
        # If body decompressed, parse ghost telemetry
        if body_data:
            result = parse_ghost_from_body(body_data)
            if result:
                ghost_info = result.get('ghost_info')
                ghost_samples = result.get('ghost_samples', [])
    
    return {
        'metadata': metadata,
        'ghost_info': ghost_info,
        'ghost_samples': ghost_samples
    }
