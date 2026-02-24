"""Ghost sample parser for CGameGhost.Data.

Based on CGameGhost.Data.Read() and CSceneVehicleCar.Sample.Read() from gbx-net.
"""

import struct
import zlib
import io
from .reader import read_uint8, read_int32, read_uint32, read_float, read_data


def find_ghost_samples_in_body(body_data):
    """Scan decompressed body for ghost samples.
    
    Args:
        body_data: Decompressed body bytes
        
    Returns:
        dict with ghost_info and ghost_samples, or None if not found
    """
    # Search for SavedMobilClassId pattern: 0x0A02B000 (CSceneVehicleCar)
    pattern = struct.pack('<I', 0x0A02B000)
    
    offset = body_data.find(pattern)
    if offset == -1:
        return None
    
    # Position after the pattern
    offset += 4
    
    if offset >= len(body_data):
        return None
    
    try:
        f = io.BytesIO(body_data[offset:])
        
        # Read IsFixedTimeStep (uint8 bool)
        is_fixed_timestep = read_uint8(f) != 0
        
        # U01 (int32)
        u01 = read_int32(f)
        
        # SamplePeriod (int32)
        sample_period = read_int32(f)
        
        # Version (int32)
        version = read_int32(f)
        
        # Sanity checks
        if sample_period < 0 or sample_period > 1000:
            return None
        if version < 0 or version > 100:
            return None
        
        # State buffer via read_data
        state_buffer = read_data(f)
        
        if len(state_buffer) == 0:
            return None
        
        # StateOffsets: num_samples
        num_samples = read_int32(f)
        
        if num_samples <= 0 or num_samples > 1000000:
            return None
        
        # first_offset
        first_offset = read_int32(f)
        
        # Compute offsets
        offsets = [first_offset]
        
        if num_samples > 1:
            # size_per_sample
            size_per_sample = read_int32(f)
            
            if size_per_sample == -1:
                # Read array of (num_samples-1) int32 deltas
                for i in range(num_samples - 1):
                    delta = read_int32(f)
                    offsets.append(offsets[-1] + delta)
            else:
                # Fixed size per sample
                for i in range(1, num_samples):
                    offsets.append(first_offset + i * size_per_sample)
        
        # StateTimes: if not fixed timestep, read array of int32
        times = []
        if not is_fixed_timestep:
            for i in range(num_samples):
                time = read_int32(f)
                times.append(time)
        else:
            # Generate times from sample_period
            for i in range(num_samples):
                times.append(i * sample_period)
        
        # Parse samples from state buffer
        samples = []
        state_stream = io.BytesIO(state_buffer)
        
        for i in range(num_samples):
            if i < len(offsets):
                # Seek to offset
                state_stream.seek(offsets[i])
                
                # First 12 bytes = Vec3 position (3 floats)
                try:
                    x = struct.unpack('<f', state_stream.read(4))[0]
                    y = struct.unpack('<f', state_stream.read(4))[0]
                    z = struct.unpack('<f', state_stream.read(4))[0]
                    
                    position = (x, y, z)
                    
                    samples.append({
                        'time_ms': times[i] if i < len(times) else i * sample_period,
                        'position': {'x': x, 'y': y, 'z': z}
                    })
                except (struct.error, IOError, IndexError):
                    # Skip malformed sample
                    pass
        
        ghost_info = {
            'is_fixed_timestep': is_fixed_timestep,
            'sample_period': sample_period,
            'version': version,
            'num_samples': num_samples
        }
        
        return {
            'ghost_info': ghost_info,
            'ghost_samples': samples
        }
    
    except (struct.error, IOError, ValueError, EOFError) as e:
        # Failed to parse
        return None


def find_zlib_ghost_data(body_data):
    """Search for zlib-compressed ghost data in body.
    
    Args:
        body_data: Decompressed body bytes
        
    Returns:
        dict with ghost_info and ghost_samples, or None if not found
    """
    # Search for zlib headers: 78 9C, 78 01, 78 DA
    zlib_headers = [b'\x78\x9C', b'\x78\x01', b'\x78\xDA']
    
    for header in zlib_headers:
        offset = 0
        while True:
            offset = body_data.find(header, offset)
            if offset == -1:
                break
            
            # Check if preceded by uncompressed_size + data_length
            # Try to decompress from this offset
            if offset >= 8:
                try:
                    # Read potential size fields before zlib header
                    size_data = body_data[offset-8:offset]
                    uncompressed_size = struct.unpack('<I', size_data[0:4])[0]
                    compressed_size = struct.unpack('<I', size_data[4:8])[0]
                    
                    # Sanity checks
                    if uncompressed_size > 100000000 or compressed_size > 100000000:
                        offset += 1
                        continue
                    
                    if compressed_size < 10:
                        offset += 1
                        continue
                    
                    # Try to decompress
                    compressed_data = body_data[offset:offset+compressed_size]
                    decompressed = zlib.decompress(compressed_data)
                    
                    if len(decompressed) > 0:
                        # Try to find ghost samples in decompressed data
                        result = find_ghost_samples_in_body(decompressed)
                        if result:
                            return result
                except (zlib.error, struct.error, ValueError):
                    pass
            
            offset += 1
    
    return None
