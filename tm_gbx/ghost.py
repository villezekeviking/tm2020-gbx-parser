"""Ghost sample parser for CPlugEntRecordData and CSceneVehicleVis.

Based on reverse-engineering of gbx-net C# reference implementation:
- CPlugEntRecordData.cs (chunk 0x0911F000)
- CSceneVehicleVis.cs (entity classId 0x0A018000)

This is the proven approach that successfully extracts 52 columns of telemetry data
from TrackMania 2020 .Ghost.Gbx files.
"""

import struct
import zlib
import io
import math
from .reader import read_uint8, read_int16, read_uint16, read_int32, read_uint32


def parse_ghost_from_body(body_data):
    """Parse ghost telemetry from decompressed body data.
    
    Args:
        body_data: Decompressed body bytes (zlib-decompressed)
        
    Returns:
        dict with ghost_info and ghost_samples (52 fields each), or None if not found
    """
    # Search for CPlugEntRecordData chunk ID: 0x0911F000 (little-endian: \x00\xf0\x11\x09)
    chunk_pattern = b'\x00\xf0\x11\x09'
    
    offset = body_data.find(chunk_pattern)
    if offset == -1:
        return None
    
    # Position after chunk ID
    offset += 4
    
    if offset + 12 > len(body_data):
        return None
    
    try:
        f = io.BytesIO(body_data[offset:])
        
        # Read version (u32)
        version = read_uint32(f)
        
        # Valid versions: 5 <= version <= 15
        if version < 5 or version > 15:
            return None
        
        # For version >= 5: read uncompressedSize (u32), dataLength (u32)
        uncompressed_size = read_uint32(f)
        data_length = read_uint32(f)
        
        # Sanity checks
        if uncompressed_size > 100000000 or data_length > 100000000:
            return None
        if data_length < 10:
            return None
        
        # Read compressed data and decompress with zlib
        compressed_data = f.read(data_length)
        if len(compressed_data) != data_length:
            return None
        
        try:
            record_data = zlib.decompress(compressed_data)
        except zlib.error:
            return None
        
        if len(record_data) == 0:
            return None
        
        # Parse the record data (version 10 format confirmed working)
        return parse_record_data(record_data, version)
    
    except (struct.error, IOError, ValueError, EOFError):
        return None


def parse_record_data(record_data, version):
    """Parse CPlugEntRecordData inner record data.
    
    Args:
        record_data: Decompressed inner record bytes
        version: Record version
        
    Returns:
        dict with ghost_info and ghost_samples
    """
    f = io.BytesIO(record_data)
    
    # Read start_time and end_time (i32)
    start_time = read_int32(f)
    end_time = read_int32(f)
    
    # EntRecordDescs array
    ent_record_descs_count = read_uint32(f)
    
    # Sanity check
    if ent_record_descs_count > 10000:
        return None
    
    ent_record_descs = []
    for _ in range(ent_record_descs_count):
        # Each desc: classId (u32), sampleSize (i32), int, int, ReadData (i32 length + bytes), int
        class_id = read_uint32(f)
        sample_size = read_int32(f)
        u01 = read_int32(f)
        u02 = read_int32(f)
        
        # ReadData: length + bytes
        data_length = read_uint32(f)
        data_bytes = f.read(data_length)
        if len(data_bytes) != data_length:
            return None
        
        u03 = read_int32(f)
        
        ent_record_descs.append({
            'class_id': class_id,
            'sample_size': sample_size,
            'u01': u01,
            'u02': u02,
            'data': data_bytes,
            'u03': u03
        })
    
    # NoticeRecordDescs array
    notice_record_descs_count = read_uint32(f)
    
    if notice_record_descs_count > 10000:
        return None
    
    notice_record_descs = []
    for _ in range(notice_record_descs_count):
        # Each notice: int, int, classId (u32) — 12 bytes total
        u01 = read_int32(f)
        u02 = read_int32(f)
        class_id = read_uint32(f)
        
        notice_record_descs.append({
            'u01': u01,
            'u02': u02,
            'class_id': class_id
        })
    
    # Entity list: parse entities looking for CSceneVehicleVis (0x0A018000)
    entities = []
    
    while True:
        # ReadByte sentinel
        has_entity = read_uint8(f)
        if has_entity != 1:
            break
        
        # Read entity type (i32)
        entity_type = read_int32(f)
        
        # u01-u04 (4x i32)
        u01 = read_int32(f)
        u02 = read_int32(f)
        u03 = read_int32(f)
        u04 = read_int32(f)
        
        # Samples: while ReadByte() == 1: time (i32) + ReadData
        samples = []
        while True:
            has_sample = read_uint8(f)
            if has_sample != 1:
                break
            
            time_ms = read_int32(f)
            
            # ReadData: length + bytes
            sample_length = read_uint32(f)
            sample_data = f.read(sample_length)
            if len(sample_data) != sample_length:
                break
            
            samples.append({
                'time_ms': time_ms,
                'data': sample_data
            })
        
        # hasNext byte
        has_next = read_uint8(f)
        
        # Samples2: while ReadByte() == 1: i32, i32, ReadData
        samples2 = []
        while True:
            has_sample2 = read_uint8(f)
            if has_sample2 != 1:
                break
            
            val1 = read_int32(f)
            val2 = read_int32(f)
            
            # ReadData
            data_length = read_uint32(f)
            data_bytes = f.read(data_length)
            if len(data_bytes) != data_length:
                break
            
            samples2.append({
                'val1': val1,
                'val2': val2,
                'data': data_bytes
            })
        
        entities.append({
            'type': entity_type,
            'u01': u01,
            'u02': u02,
            'u03': u03,
            'u04': u04,
            'samples': samples,
            'has_next': has_next,
            'samples2': samples2
        })
    
    # Find CSceneVehicleVis entity (classId 0x0A018000)
    vehicle_entity = None
    for entity in entities:
        if entity['type'] == 0x0A018000:
            vehicle_entity = entity
            break
    
    if not vehicle_entity:
        return None
    
    # Parse CSceneVehicleVis samples (107 bytes each)
    ghost_samples = []
    for sample in vehicle_entity['samples']:
        parsed_sample = parse_vehicle_vis_sample(sample['time_ms'], sample['data'])
        if parsed_sample:
            ghost_samples.append(parsed_sample)
    
    ghost_info = {
        'start_time': start_time,
        'end_time': end_time,
        'num_samples': len(ghost_samples),
        'sample_period_ms': 50,  # TrackMania samples at 20Hz (50ms)
        'version': version
    }
    
    return {
        'ghost_info': ghost_info,
        'ghost_samples': ghost_samples
    }


def parse_vehicle_vis_sample(time_ms, sample_data):
    """Parse a CSceneVehicleVis sample (107 bytes).
    
    Based on CSceneVehicleVis.cs from gbx-net reference implementation.
    
    Args:
        time_ms: Sample timestamp in milliseconds
        sample_data: 107-byte sample data
        
    Returns:
        dict with all 52 telemetry fields
    """
    if len(sample_data) != 107:
        return None
    
    try:
        # Helper to read specific bytes
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
        # Bytes 47-58: Vec3 position (3x f32)
        x = read_f32(47)
        y = read_f32(51)
        z = read_f32(55)
        
        # Bytes 59-60: angle (u16) → angle * π / 65535
        angle_raw = read_u16(59)
        angle = angle_raw * math.pi / 65535.0
        
        # Bytes 61-62: axisHeading (i16) → axisHeading * π / 32767
        axis_heading_raw = read_i16(61)
        axis_heading = axis_heading_raw * math.pi / 32767.0
        
        # Bytes 63-64: axisPitch (i16) → axisPitch / 32767 * π/2
        axis_pitch_raw = read_i16(63)
        axis_pitch = (axis_pitch_raw / 32767.0) * (math.pi / 2.0)
        
        # Bytes 65-66: speed (i16) → exp(speed / 1000.0)
        speed_raw = read_i16(65)
        speed = math.exp(speed_raw / 1000.0)
        
        # Bytes 67: velocityHeading (i8) → velHeading / 127 * π
        vel_heading_raw = read_i8(67)
        vel_heading = (vel_heading_raw / 127.0) * math.pi
        
        # Bytes 68: velocityPitch (i8) → velPitch / 127 * π/2
        vel_pitch_raw = read_i8(68)
        vel_pitch = (vel_pitch_raw / 127.0) * (math.pi / 2.0)
        
        # Quaternion from axis-angle
        ax = math.sin(angle) * math.cos(axis_pitch) * math.cos(axis_heading)
        ay = math.sin(angle) * math.cos(axis_pitch) * math.sin(axis_heading)
        az = math.sin(angle) * math.sin(axis_pitch)
        qw = math.cos(angle)
        
        # Convert quaternion to Euler angles (pitch, yaw, roll in degrees)
        # Using standard aerospace convention
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
        
        # Convert to degrees
        pitch_deg = math.degrees(pitch)
        yaw_deg = math.degrees(yaw)
        roll_deg = math.degrees(roll)
        
        # Velocity vector
        vel_x = speed * math.cos(vel_pitch) * math.cos(vel_heading)
        vel_y = speed * math.cos(vel_pitch) * math.sin(vel_heading)
        vel_z = speed * math.sin(vel_pitch)
        
        # Individual byte fields
        # Byte 2-3: SideSpeed (u16) → ((val / 65536) - 0.5) * 2000
        side_speed_raw = read_u16(2)
        side_speed = ((side_speed_raw / 65536.0) - 0.5) * 2000.0
        
        # Byte 5: RPM (u8)
        rpm = read_u8(5)
        
        # Bytes 6-13: Wheel rotation (FL rot, FL count, FR rot, FR count, RR rot, RR count, RL rot, RL count)
        # Formula: (rot/255 * 2π) + (count * 2π)
        fl_wheel_rot_raw = read_u8(6)
        fl_wheel_count = read_u8(7)
        fl_wheel_rot = (fl_wheel_rot_raw / 255.0) * (2 * math.pi) + (fl_wheel_count * 2 * math.pi)
        
        fr_wheel_rot_raw = read_u8(8)
        fr_wheel_count = read_u8(9)
        fr_wheel_rot = (fr_wheel_rot_raw / 255.0) * (2 * math.pi) + (fr_wheel_count * 2 * math.pi)
        
        rr_wheel_rot_raw = read_u8(10)
        rr_wheel_count = read_u8(11)
        rr_wheel_rot = (rr_wheel_rot_raw / 255.0) * (2 * math.pi) + (rr_wheel_count * 2 * math.pi)
        
        rl_wheel_rot_raw = read_u8(12)
        rl_wheel_count = read_u8(13)
        rl_wheel_rot = (rl_wheel_rot_raw / 255.0) * (2 * math.pi) + (rl_wheel_count * 2 * math.pi)
        
        # Byte 14: Steer (u8) → ((val / 255) - 0.5) * 2
        steer_raw = read_u8(14)
        steer = ((steer_raw / 255.0) - 0.5) * 2.0
        
        # Byte 18: Brake (u8) → val / 255
        brake_raw = read_u8(18)
        brake = brake_raw / 255.0
        
        # Byte 15: Gas component (u8) → val / 255 + brake
        gas_raw = read_u8(15)
        gas = (gas_raw / 255.0) + brake
        
        # Byte 21: TurboTime (u8) → val / 255
        turbo_time_raw = read_u8(21)
        turbo_time = turbo_time_raw / 255.0
        
        # Bytes 23,25,27,29: DampenLen FL,FR,RR,RL → ((val / 255) - 0.5) * 4
        fl_dampen_raw = read_u8(23)
        fl_dampen = ((fl_dampen_raw / 255.0) - 0.5) * 4.0
        
        fr_dampen_raw = read_u8(25)
        fr_dampen = ((fr_dampen_raw / 255.0) - 0.5) * 4.0
        
        rr_dampen_raw = read_u8(27)
        rr_dampen = ((rr_dampen_raw / 255.0) - 0.5) * 4.0
        
        rl_dampen_raw = read_u8(29)
        rl_dampen = ((rl_dampen_raw / 255.0) - 0.5) * 4.0
        
        # Bytes 24,26,28,30: GroundContactMaterial FL,FR,RR,RL (u8 raw)
        fl_ground_mat = read_u8(24)
        fr_ground_mat = read_u8(26)
        rr_ground_mat = read_u8(28)
        rl_ground_mat = read_u8(30)
        
        # Byte 31: IsTurbo → (val & 0x82) != 0
        is_turbo_raw = read_u8(31)
        is_turbo = (is_turbo_raw & 0x82) != 0
        
        # Bytes 32,33: SlipCoef → FL: byte1 & 0x40, FR: byte2 & 0x01, RR: byte2 & 0x04, RL: byte2 & 0x10
        slip_byte1 = read_u8(32)
        slip_byte2 = read_u8(33)
        fl_slip = (slip_byte1 & 0x40) != 0
        fr_slip = (slip_byte2 & 0x01) != 0
        rr_slip = (slip_byte2 & 0x04) != 0
        rl_slip = (slip_byte2 & 0x10) != 0
        
        # Byte 76: IsTopContact → (val & 0x20) != 0
        is_top_contact_raw = read_u8(76)
        is_top_contact = (is_top_contact_raw & 0x20) != 0
        
        # Bytes 81-84: Ice FL,FR,RR,RL → val / 255
        fl_ice_raw = read_u8(81)
        fl_ice = fl_ice_raw / 255.0
        
        fr_ice_raw = read_u8(82)
        fr_ice = fr_ice_raw / 255.0
        
        rr_ice_raw = read_u8(83)
        rr_ice = rr_ice_raw / 255.0
        
        rl_ice_raw = read_u8(84)
        rl_ice = rl_ice_raw / 255.0
        
        # Byte 89: GroundContact/Reactor flags
        reactor_flags = read_u8(89)
        is_ground_contact = (reactor_flags & 0x01) != 0
        reactor_ground = (reactor_flags & 0x04) != 0
        reactor_up = (reactor_flags & 0x08) != 0
        reactor_down = (reactor_flags & 0x10) != 0
        reactor_lvl1 = (reactor_flags & 0x20) != 0
        reactor_lvl2 = (reactor_flags & 0x40) != 0
        
        # Combine reactor flags into state and boost
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
        
        # Byte 90: ReactorAirControl
        reactor_control = read_u8(90)
        reactor_pedal_accel = (reactor_control & 0x20) != 0
        reactor_pedal_none = (reactor_control & 0x10) != 0
        reactor_steer_left = (reactor_control & 0x80) != 0
        reactor_steer_none = (reactor_control & 0x40) != 0
        
        # Encode reactor pedal and steer as integers
        reactor_pedal = 1 if reactor_pedal_accel else (0 if reactor_pedal_none else -1)
        reactor_steer = -1 if reactor_steer_left else (0 if reactor_steer_none else 1)
        
        # Byte 91: Gear → val / 5.0
        gear_raw = read_u8(91)
        gear = gear_raw / 5.0
        
        # Bytes 93,95,97,99: Dirt FL,FR,RR,RL → val / 255
        fl_dirt_raw = read_u8(93)
        fl_dirt = fl_dirt_raw / 255.0
        
        fr_dirt_raw = read_u8(95)
        fr_dirt = fr_dirt_raw / 255.0
        
        rr_dirt_raw = read_u8(97)
        rr_dirt = rr_dirt_raw / 255.0
        
        rl_dirt_raw = read_u8(99)
        rl_dirt = rl_dirt_raw / 255.0
        
        # Byte 101: Wetness → val / 255
        wetness_raw = read_u8(101)
        wetness = wetness_raw / 255.0
        
        # Byte 102: SimulationTimeCoef → val / 255
        sim_time_coef_raw = read_u8(102)
        sim_time_coef = sim_time_coef_raw / 255.0
        
        # Return all 52 fields
        return {
            'time_ms': time_ms,
            'time_s': time_ms / 1000.0,
            'x': x,
            'y': y,
            'z': z,
            'speed': speed,
            'side_speed': side_speed,
            'vel_x': vel_x,
            'vel_y': vel_y,
            'vel_z': vel_z,
            'pitch_deg': pitch_deg,
            'yaw_deg': yaw_deg,
            'roll_deg': roll_deg,
            'steer': steer,
            'gas': gas,
            'brake': brake,
            'gear': gear,
            'rpm': rpm,
            'is_turbo': is_turbo,
            'turbo_time': turbo_time,
            'is_ground_contact': is_ground_contact,
            'is_top_contact': is_top_contact,
            'reactor_state': reactor_state,
            'reactor_boost': reactor_boost,
            'reactor_pedal': reactor_pedal,
            'reactor_steer': reactor_steer,
            'sim_time_coef': sim_time_coef,
            'wetness': wetness,
            'fl_dampen': fl_dampen,
            'fr_dampen': fr_dampen,
            'rr_dampen': rr_dampen,
            'rl_dampen': rl_dampen,
            'fl_ice': fl_ice,
            'fr_ice': fr_ice,
            'rr_ice': rr_ice,
            'rl_ice': rl_ice,
            'fl_dirt': fl_dirt,
            'fr_dirt': fr_dirt,
            'rr_dirt': rr_dirt,
            'rl_dirt': rl_dirt,
            'fl_slip': fl_slip,
            'fr_slip': fr_slip,
            'rr_slip': rr_slip,
            'rl_slip': rl_slip,
            'fl_ground_mat': fl_ground_mat,
            'fr_ground_mat': fr_ground_mat,
            'rr_ground_mat': rr_ground_mat,
            'rl_ground_mat': rl_ground_mat,
            'fl_wheel_rot': fl_wheel_rot,
            'fr_wheel_rot': fr_wheel_rot,
            'rr_wheel_rot': rr_wheel_rot,
            'rl_wheel_rot': rl_wheel_rot
        }
    
    except (struct.error, ValueError, IndexError):
        return None
