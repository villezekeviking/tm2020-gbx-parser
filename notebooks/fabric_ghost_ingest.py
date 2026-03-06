"""
Microsoft Fabric Notebook: TrackMania 2020 Ghost Telemetry Ingestion

This notebook reads .Replay.Gbx and .Ghost.Gbx files from the Lakehouse Files
area and creates two Delta tables with ghost metadata and telemetry data
(52 fields per sample). The parser works on both file types since it locates
the CPlugEntRecordData chunk by byte signature rather than relying on the
file extension.

Prerequisites:
- .Replay.Gbx (or .Ghost.Gbx) files in /lakehouse/default/Files/replays/
  (as written by fabric_replay_download.py, in {year}/{month}/{map_id}/ subfolders)
- Write access to /lakehouse/default/Tables/

Output:
- silver_replay_header:    One row per ghost file (metadata + ghost info)
- silver_replay_telemetry: One row per telemetry sample (52 fields)

Note: The 'spark' variable is pre-defined in Fabric Notebooks.
"""

# ========================================
# Cell 1: Parser Functions (Inline)
# ========================================

# Since tm_gbx may not be pip-installable in Fabric, we include the parsing logic inline

import struct
import zlib
import io
import math
import os
import hashlib
from datetime import datetime


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


def read_string(f):
    """Read length-prefixed string."""
    length = read_uint32(f)
    if length == 0:
        return ""
    if length > 100000:
        return ""
    data = f.read(length)
    if len(data) != length:
        raise EOFError(f"Failed to read string of length {length}")
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        return data.decode('latin-1', errors='ignore')


class _LookbackReader:
    """Manages GBX lookback string reading (string interning system)."""

    def __init__(self):
        self.id_version = None
        self.lookback_strings = {}
        self.counter = 0

    def read_id(self, f):
        if self.id_version is None:
            self.id_version = read_uint32(f)
            if self.id_version < 3:
                return ""
        index = read_uint32(f)
        if index == 0xFFFFFFFF:
            return ""
        high_bits = (index >> 30) & 0x3
        if high_bits != 1 and high_bits != 2:
            return ""
        masked_index = index & 0x3FFFFFFF
        if masked_index != 0:
            return self.lookback_strings.get(masked_index, "")
        string = read_string(f)
        self.counter += 1
        self.lookback_strings[self.counter] = string
        return string

    def read_ident(self, f):
        id_str = self.read_id(f)
        collection = self.read_id(f)
        author = self.read_id(f)
        return (id_str, collection, author)


def _parse_gbx_header_metadata(f, user_data_size):
    """Parse GBX header user-data chunks to extract replay metadata.

    Handles the chunk list structure that appears after the main GBX header
    fields.  Returns a dict with any of: map_uid, map_author, race_time_ms,
    player_nickname, player_login.  Missing fields are simply absent from the
    dict — callers should use .get() with a default.
    """
    if user_data_size == 0:
        return {}

    user_data_start = f.tell()
    metadata = {}

    try:
        num_chunks = read_uint32(f)
        if num_chunks == 0 or num_chunks > 1000:
            f.seek(user_data_start + user_data_size)
            return {}

        chunk_headers = []
        for _ in range(num_chunks):
            chunk_id = read_uint32(f)
            chunk_size_raw = read_int32(f)
            chunk_size = chunk_size_raw & 0x7FFFFFFF
            chunk_headers.append({'id': chunk_id, 'size': chunk_size})

        lookback = _LookbackReader()

        for chunk in chunk_headers:
            chunk_id = chunk['id']
            chunk_size = chunk['size']
            chunk_start = f.tell()

            try:
                if chunk_id == 0x03093000:
                    # CGameCtnReplayRecord header — contains map info, time, player
                    chunk_version = read_uint32(f)
                    if chunk_version >= 4 and chunk_version != 9999:
                        map_info = lookback.read_ident(f)
                        if map_info[0]:
                            metadata['map_uid'] = map_info[0]
                        if map_info[2]:
                            metadata['map_author'] = map_info[2]
                    time = read_int32(f)
                    if time >= 0:
                        metadata['race_time_ms'] = time
                    nickname = read_string(f)
                    if nickname:
                        metadata['player_nickname'] = nickname
                    if chunk_version >= 6:
                        login = read_string(f)
                        if login:
                            metadata['player_login'] = login
            except Exception:
                pass  # Skip unreadable individual chunks; outer loop continues

            f.seek(chunk_start + chunk_size)

    except Exception as e:
        print(f"⚠ Could not parse GBX header metadata: {e}")
    finally:
        f.seek(user_data_start + user_data_size)

    return metadata


def parse_vehicle_vis_sample(time_ms, sample_data):
    """Parse a CSceneVehicleVis sample (107 bytes) into 52 telemetry fields."""
    if len(sample_data) != 107:
        return None
    
    try:
        # Helper functions
        def read_u8(offset):
            return sample_data[offset]
        
        def read_i8(offset):
            return struct.unpack('b', sample_data[offset:offset+1])[0]
        
        def read_u16(offset):
            return struct.unpack('<H', sample_data[offset:offset+2])[0]
        
        def read_i16(offset):
            return struct.unpack('<h', sample_data[offset:offset+2])[0]
        
        def read_f32(offset):
            return struct.unpack('<f', sample_data[offset:offset+4])[0]
        
        # Position & Transform (ReadTransform at byte offset 47)
        x = read_f32(47)
        y = read_f32(51)
        z = read_f32(55)
        
        # Rotation (axis-angle)
        angle_raw = read_u16(59)
        angle = angle_raw * math.pi / 65535.0
        
        axis_heading_raw = read_i16(61)
        axis_heading = axis_heading_raw * math.pi / 32767.0
        
        axis_pitch_raw = read_i16(63)
        axis_pitch = (axis_pitch_raw / 32767.0) * (math.pi / 2.0)
        
        # Speed (exponential encoding)
        speed_raw = read_i16(65)
        speed = math.exp(speed_raw / 1000.0)
        
        # Velocity direction
        vel_heading_raw = read_i8(67)
        vel_heading = (vel_heading_raw / 127.0) * math.pi
        
        vel_pitch_raw = read_i8(68)
        vel_pitch = (vel_pitch_raw / 127.0) * (math.pi / 2.0)
        
        # Quaternion from axis-angle
        ax = math.sin(angle) * math.cos(axis_pitch) * math.cos(axis_heading)
        ay = math.sin(angle) * math.cos(axis_pitch) * math.sin(axis_heading)
        az = math.sin(angle) * math.sin(axis_pitch)
        qw = math.cos(angle)
        
        # Convert quaternion to Euler angles
        sinr_cosp = 2.0 * (qw * ax + ay * az)
        cosr_cosp = 1.0 - 2.0 * (ax * ax + ay * ay)
        roll = math.atan2(sinr_cosp, cosr_cosp)
        
        sinp = 2.0 * (qw * ay - az * ax)
        if abs(sinp) >= 1:
            pitch = math.copysign(math.pi / 2, sinp)
        else:
            pitch = math.asin(sinp)
        
        siny_cosp = 2.0 * (qw * az + ax * ay)
        cosy_cosp = 1.0 - 2.0 * (ay * ay + az * az)
        yaw = math.atan2(siny_cosp, cosy_cosp)
        
        pitch_deg = math.degrees(pitch)
        yaw_deg = math.degrees(yaw)
        roll_deg = math.degrees(roll)
        
        # Velocity vector
        vel_x = speed * math.cos(vel_pitch) * math.cos(vel_heading)
        vel_y = speed * math.cos(vel_pitch) * math.sin(vel_heading)
        vel_z = speed * math.sin(vel_pitch)
        
        # Other fields
        side_speed_raw = read_u16(2)
        side_speed = ((side_speed_raw / 65536.0) - 0.5) * 2000.0
        
        rpm = read_u8(5)
        
        # Wheel rotations
        fl_wheel_rot = (read_u8(6) / 255.0) * (2 * math.pi) + (read_u8(7) * 2 * math.pi)
        fr_wheel_rot = (read_u8(8) / 255.0) * (2 * math.pi) + (read_u8(9) * 2 * math.pi)
        rr_wheel_rot = (read_u8(10) / 255.0) * (2 * math.pi) + (read_u8(11) * 2 * math.pi)
        rl_wheel_rot = (read_u8(12) / 255.0) * (2 * math.pi) + (read_u8(13) * 2 * math.pi)
        
        steer = ((read_u8(14) / 255.0) - 0.5) * 2.0
        brake = read_u8(18) / 255.0
        gas = (read_u8(15) / 255.0) + brake
        turbo_time = read_u8(21) / 255.0
        
        # Suspension
        fl_dampen = ((read_u8(23) / 255.0) - 0.5) * 4.0
        fr_dampen = ((read_u8(25) / 255.0) - 0.5) * 4.0
        rr_dampen = ((read_u8(27) / 255.0) - 0.5) * 4.0
        rl_dampen = ((read_u8(29) / 255.0) - 0.5) * 4.0
        
        # Ground contact materials
        fl_ground_mat = read_u8(24)
        fr_ground_mat = read_u8(26)
        rr_ground_mat = read_u8(28)
        rl_ground_mat = read_u8(30)
        
        # Turbo
        is_turbo = (read_u8(31) & 0x82) != 0
        
        # Slip
        slip_byte1 = read_u8(32)
        slip_byte2 = read_u8(33)
        fl_slip = (slip_byte1 & 0x40) != 0
        fr_slip = (slip_byte2 & 0x01) != 0
        rr_slip = (slip_byte2 & 0x04) != 0
        rl_slip = (slip_byte2 & 0x10) != 0
        
        # Contact
        is_top_contact = (read_u8(76) & 0x20) != 0
        
        # Ice
        fl_ice = read_u8(81) / 255.0
        fr_ice = read_u8(82) / 255.0
        rr_ice = read_u8(83) / 255.0
        rl_ice = read_u8(84) / 255.0
        
        # Reactor
        reactor_flags = read_u8(89)
        is_ground_contact = (reactor_flags & 0x01) != 0
        reactor_ground = (reactor_flags & 0x04) != 0
        reactor_up = (reactor_flags & 0x08) != 0
        reactor_down = (reactor_flags & 0x10) != 0
        reactor_lvl1 = (reactor_flags & 0x20) != 0
        reactor_lvl2 = (reactor_flags & 0x40) != 0
        
        reactor_state = 0
        if reactor_ground:
            reactor_state = 1
        elif reactor_up:
            reactor_state = 2
        elif reactor_down:
            reactor_state = 3
        
        reactor_boost = 0
        if reactor_lvl1:
            reactor_boost = 1
        elif reactor_lvl2:
            reactor_boost = 2
        
        reactor_control = read_u8(90)
        reactor_pedal_accel = (reactor_control & 0x20) != 0
        reactor_pedal_none = (reactor_control & 0x10) != 0
        reactor_steer_left = (reactor_control & 0x80) != 0
        reactor_steer_none = (reactor_control & 0x40) != 0
        
        reactor_pedal = 1 if reactor_pedal_accel else (0 if reactor_pedal_none else -1)
        reactor_steer = -1 if reactor_steer_left else (0 if reactor_steer_none else 1)
        
        gear = read_u8(91) / 5.0
        
        # Dirt
        fl_dirt = read_u8(93) / 255.0
        fr_dirt = read_u8(95) / 255.0
        rr_dirt = read_u8(97) / 255.0
        rl_dirt = read_u8(99) / 255.0
        
        wetness = read_u8(101) / 255.0
        sim_time_coef = read_u8(102) / 255.0
        
        return {
            'time_ms': time_ms,
            'time_s': time_ms / 1000.0,
            'x': x, 'y': y, 'z': z,
            'speed': speed,
            'side_speed': side_speed,
            'vel_x': vel_x, 'vel_y': vel_y, 'vel_z': vel_z,
            'pitch_deg': pitch_deg, 'yaw_deg': yaw_deg, 'roll_deg': roll_deg,
            'steer': steer, 'gas': gas, 'brake': brake, 'gear': gear, 'rpm': rpm,
            'is_turbo': is_turbo, 'turbo_time': turbo_time,
            'is_ground_contact': is_ground_contact, 'is_top_contact': is_top_contact,
            'reactor_state': reactor_state, 'reactor_boost': reactor_boost,
            'reactor_pedal': reactor_pedal, 'reactor_steer': reactor_steer,
            'sim_time_coef': sim_time_coef, 'wetness': wetness,
            'fl_dampen': fl_dampen, 'fr_dampen': fr_dampen, 'rr_dampen': rr_dampen, 'rl_dampen': rl_dampen,
            'fl_ice': fl_ice, 'fr_ice': fr_ice, 'rr_ice': rr_ice, 'rl_ice': rl_ice,
            'fl_dirt': fl_dirt, 'fr_dirt': fr_dirt, 'rr_dirt': rr_dirt, 'rl_dirt': rl_dirt,
            'fl_slip': fl_slip, 'fr_slip': fr_slip, 'rr_slip': rr_slip, 'rl_slip': rl_slip,
            'fl_ground_mat': fl_ground_mat, 'fr_ground_mat': fr_ground_mat, 'rr_ground_mat': rr_ground_mat, 'rl_ground_mat': rl_ground_mat,
            'fl_wheel_rot': fl_wheel_rot, 'fr_wheel_rot': fr_wheel_rot, 'rr_wheel_rot': rr_wheel_rot, 'rl_wheel_rot': rl_wheel_rot
        }
    except (struct.error, ValueError, IndexError):
        return None


def parse_gbx_file(filepath):
    """Parse a .Ghost.Gbx file and return metadata + telemetry."""
    with open(filepath, 'rb') as f:
        # Parse header
        magic = f.read(3)
        if magic != b'GBX':
            return None
        
        version = read_uint16(f)
        format_byte = read_uint8(f)
        ref_table_compressed = read_uint8(f)
        body_compressed = read_uint8(f)
        
        if version >= 4:
            unknown_byte = read_uint8(f)
        
        class_id = read_uint32(f)
        user_data_size = read_uint32(f)
        
        # Parse header chunks to extract map_uid, race_time_ms, player info
        header_metadata = _parse_gbx_header_metadata(f, user_data_size)
        
        # Skip ref table
        num_external = read_int32(f)
        if num_external > 0:
            for _ in range(num_external):
                flags = read_int32(f)
                if (flags & 0x4) != 0:
                    file_path = read_string(f)
                else:
                    file_node_index = read_int32(f)
                if (flags & 0x8) != 0:
                    resource_index = read_int32(f)
                node_index = read_int32(f)
                use_flags = read_int32(f)
                if (flags & 0x10) != 0:
                    folder_dep_count = read_int32(f)
        
        # Read body (zlib-compressed for .Ghost.Gbx files)
        if body_compressed != 0x43:
            return None
        
        uncompressed_size = read_uint32(f)
        compressed_size = read_uint32(f)
        compressed_data = f.read(compressed_size)
        
        try:
            body_data = zlib.decompress(compressed_data)
        except zlib.error:
            return None
        
        # Search for CPlugEntRecordData chunk (0x0911F000)
        chunk_pattern = b'\x00\xf0\x11\x09'
        offset = body_data.find(chunk_pattern)
        if offset == -1:
            return None
        
        offset += 4
        f_body = io.BytesIO(body_data[offset:])
        
        # Read version
        record_version = read_uint32(f_body)
        if record_version < 5 or record_version > 15:
            return None
        
        # Read inner compressed data
        uncompressed_size2 = read_uint32(f_body)
        data_length = read_uint32(f_body)
        compressed_data2 = f_body.read(data_length)
        
        try:
            record_data = zlib.decompress(compressed_data2)
        except zlib.error:
            return None
        
        # Parse record data
        f_record = io.BytesIO(record_data)
        
        start_time = read_int32(f_record)
        end_time = read_int32(f_record)
        
        # Skip EntRecordDescs
        ent_count = read_uint32(f_record)
        for _ in range(ent_count):
            class_id = read_uint32(f_record)
            sample_size = read_int32(f_record)
            read_int32(f_record)
            read_int32(f_record)
            data_len = read_uint32(f_record)
            f_record.read(data_len)
            read_int32(f_record)
        
        # Skip NoticeRecordDescs
        notice_count = read_uint32(f_record)
        for _ in range(notice_count):
            read_int32(f_record)
            read_int32(f_record)
            read_uint32(f_record)
        
        # Parse entities
        samples = []
        while True:
            has_entity = read_uint8(f_record)
            if has_entity != 1:
                break
            
            entity_type = read_int32(f_record)
            read_int32(f_record)  # u01
            read_int32(f_record)  # u02
            read_int32(f_record)  # u03
            read_int32(f_record)  # u04
            
            # Entity samples
            entity_samples = []
            while True:
                has_sample = read_uint8(f_record)
                if has_sample != 1:
                    break
                
                time_ms = read_int32(f_record)
                sample_length = read_uint32(f_record)
                sample_data = f_record.read(sample_length)
                
                if entity_type == 0x0A018000:  # CSceneVehicleVis
                    parsed = parse_vehicle_vis_sample(time_ms, sample_data)
                    if parsed:
                        entity_samples.append(parsed)
            
            read_uint8(f_record)  # hasNext
            
            # Skip samples2
            while True:
                has_sample2 = read_uint8(f_record)
                if has_sample2 != 1:
                    break
                read_int32(f_record)
                read_int32(f_record)
                data_len = read_uint32(f_record)
                f_record.read(data_len)
            
            if entity_type == 0x0A018000:
                samples = entity_samples
        
        return {
            'start_time': start_time,
            'end_time': end_time,
            'num_samples': len(samples),
            'samples': samples,
            'header_metadata': header_metadata
        }


# ========================================
# Cell 2: Read and Parse .Replay.Gbx and .Ghost.Gbx Files
# ========================================

# Input directory — matches the output of fabric_replay_download.py
input_dir = "/lakehouse/default/Files/replays/"

# Recursively find all .Replay.Gbx and .Ghost.Gbx files
gbx_files = []
for root, dirs, files in os.walk(input_dir):
    for f in files:
        if f.endswith(".Replay.Gbx") or f.endswith(".Ghost.Gbx"):
            gbx_files.append(os.path.join(root, f))

print(f"Found {len(gbx_files)} .Replay.Gbx / .Ghost.Gbx files")

# Parse all files
parsed_ghosts = []

for filepath in gbx_files:
    try:
        result = parse_gbx_file(filepath)
        if result:
            file_name = os.path.basename(filepath)
            # Generate ghost_id from filename hash
            ghost_id = hashlib.md5(file_name.encode()).hexdigest()
            
            parsed_ghosts.append({
                'ghost_id': ghost_id,
                'file_name': file_name,
                'start_time': result['start_time'],
                'end_time': result['end_time'],
                'num_samples': result['num_samples'],
                'samples': result['samples'],
                'header_metadata': result['header_metadata']
            })
            print(f"✓ Parsed {file_name}: {result['num_samples']} samples")
        else:
            print(f"✗ Failed to parse {os.path.basename(filepath)}")
    except Exception as e:
        print(f"✗ Error parsing {os.path.basename(filepath)}: {e}")

print(f"\nSuccessfully parsed {len(parsed_ghosts)} ghost files")


# ========================================
# Cell 3: Create silver_replay_header DataFrame
# ========================================

if len(parsed_ghosts) > 0:
    header_rows = []
    
    for ghost in parsed_ghosts:
        meta = ghost['header_metadata']
        
        # race_time_ms: use header value when available, fall back to end-start
        raw_race_time = meta.get('race_time_ms', ghost['end_time'] - ghost['start_time'])
        if raw_race_time < 0:
            print(f"⚠ Negative race_time_ms ({raw_race_time}) for {ghost['file_name']} — clamping to 0")
        race_time_ms = max(int(raw_race_time), 0)
        
        header_rows.append({
            'replay_id': str(ghost['ghost_id']),
            'file_name': str(ghost['file_name']),
            'source': 'player',
            'player_nickname': str(meta.get('player_nickname', '')),
            'player_login': str(meta.get('player_login', '')),
            'map_uid': str(meta.get('map_uid', '')),
            'map_author': str(meta.get('map_author', '')),
            'race_time_ms': race_time_ms,
            'race_time_s': round(race_time_ms / 1000.0, 3),
            'start_time_ms': int(ghost['start_time']),
            'end_time_ms': int(ghost['end_time']),
            'num_samples': int(ghost['num_samples']),
            'sample_period_ms': int(50),
            'ingested_at': datetime.now()
        })
    
    df_header = spark.createDataFrame(header_rows)
    
    # Overwrite mode — safe to rerun without duplicates
    df_header.write.format("delta").mode("overwrite").save("/lakehouse/default/Tables/silver_replay_header")
    
    print(f"✓ Wrote {len(header_rows)} rows to silver_replay_header table")
    
    # Show sample
    df_header.show(5, truncate=False)
else:
    print("No ghost files to ingest")


# ========================================
# Cell 4: Create silver_replay_telemetry DataFrame
# ========================================

if len(parsed_ghosts) > 0:
    telemetry_rows = []
    
    for ghost in parsed_ghosts:
        replay_id = str(ghost['ghost_id'])
        
        for sample in ghost['samples']:
            telemetry_rows.append({
                'replay_id': replay_id,
                'time_ms': int(sample['time_ms']),
                'time_s': float(sample['time_s']),
                'x': float(sample['x']),
                'y': float(sample['y']),
                'z': float(sample['z']),
                'speed': float(sample['speed']),
                'side_speed': float(sample['side_speed']),
                'vel_x': float(sample['vel_x']),
                'vel_y': float(sample['vel_y']),
                'vel_z': float(sample['vel_z']),
                'pitch_deg': float(sample['pitch_deg']),
                'yaw_deg': float(sample['yaw_deg']),
                'roll_deg': float(sample['roll_deg']),
                'steer': float(sample['steer']),
                'gas': float(sample['gas']),
                'brake': float(sample['brake']),
                'gear': float(sample['gear']),
                'rpm': int(sample['rpm']),
                'is_turbo': bool(sample['is_turbo']),
                'turbo_time': float(sample['turbo_time']),
                'is_ground_contact': bool(sample['is_ground_contact']),
                'is_top_contact': bool(sample['is_top_contact']),
                'reactor_state': int(sample['reactor_state']),
                'reactor_boost': int(sample['reactor_boost']),
                'reactor_pedal': int(sample['reactor_pedal']),
                'reactor_steer': int(sample['reactor_steer']),
                'sim_time_coef': float(sample['sim_time_coef']),
                'wetness': float(sample['wetness']),
                'fl_dampen': float(sample['fl_dampen']),
                'fr_dampen': float(sample['fr_dampen']),
                'rr_dampen': float(sample['rr_dampen']),
                'rl_dampen': float(sample['rl_dampen']),
                'fl_ice': float(sample['fl_ice']),
                'fr_ice': float(sample['fr_ice']),
                'rr_ice': float(sample['rr_ice']),
                'rl_ice': float(sample['rl_ice']),
                'fl_dirt': float(sample['fl_dirt']),
                'fr_dirt': float(sample['fr_dirt']),
                'rr_dirt': float(sample['rr_dirt']),
                'rl_dirt': float(sample['rl_dirt']),
                'fl_slip': bool(sample['fl_slip']),
                'fr_slip': bool(sample['fr_slip']),
                'rr_slip': bool(sample['rr_slip']),
                'rl_slip': bool(sample['rl_slip']),
                'fl_ground_mat': int(sample['fl_ground_mat']),
                'fr_ground_mat': int(sample['fr_ground_mat']),
                'rr_ground_mat': int(sample['rr_ground_mat']),
                'rl_ground_mat': int(sample['rl_ground_mat']),
                'fl_wheel_rot': float(sample['fl_wheel_rot']),
                'fr_wheel_rot': float(sample['fr_wheel_rot']),
                'rr_wheel_rot': float(sample['rr_wheel_rot']),
                'rl_wheel_rot': float(sample['rl_wheel_rot'])
            })
    
    print(f"Prepared {len(telemetry_rows)} telemetry rows")
    
    # Create DataFrame in batches to avoid memory issues.
    # First batch uses overwrite (clears the table), subsequent batches use append.
    # This ensures we never accumulate duplicate rows across reruns while still
    # handling large datasets that don't fit in a single DataFrame.
    batch_size = 100000
    for i in range(0, len(telemetry_rows), batch_size):
        batch = telemetry_rows[i:i+batch_size]
        df_telemetry = spark.createDataFrame(batch)
        
        # Overwrite on first batch (clears old data); append for the rest
        write_mode = "overwrite" if i == 0 else "append"
        df_telemetry.write.format("delta").mode(write_mode).save("/lakehouse/default/Tables/silver_replay_telemetry")
        
        print(f"✓ Wrote batch {i//batch_size + 1}: {len(batch)} rows")
    
    print(f"\n✓ Total telemetry rows written: {len(telemetry_rows)}")
    
    # Show sample
    df_sample = spark.read.format("delta").load("/lakehouse/default/Tables/silver_replay_telemetry")
    df_sample.show(10, truncate=False)
else:
    print("No telemetry data to ingest")


# ========================================
# Cell 5: Summary
# ========================================

print("=" * 60)
print("INGESTION SUMMARY")
print("=" * 60)
print(f"Files processed: {len(gbx_files)}")
print(f"Files successfully parsed: {len(parsed_ghosts)}")
print(f"Total samples ingested: {sum(g['num_samples'] for g in parsed_ghosts)}")
print("=" * 60)

# Query the tables
print("\nReplay Header Table:")
spark.read.format("delta").load("/lakehouse/default/Tables/silver_replay_header").show(10, truncate=False)

print("\nReplay Telemetry Table (sample):")
spark.sql("""
    SELECT replay_id, time_s, x, y, z, speed, steer, gas, brake
    FROM delta.`/lakehouse/default/Tables/silver_replay_telemetry`
    LIMIT 20
""").show(20, truncate=False)
