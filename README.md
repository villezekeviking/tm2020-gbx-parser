# TM2020 GBX Parser

Pure-Python parser for TrackMania 2020 GBX files (`.Ghost.Gbx` ghost files and `.Gbx` replay files). Extracts rich 52-field telemetry data from ghost recordings. Designed for Microsoft Fabric Lakehouse pipelines and standard Python environments.

## Features

- **Pure Python**: No external dependencies (uses only Python stdlib: struct, zlib, io, math, os, hashlib, datetime)
- **Rich Telemetry**: Extracts 52 fields per sample including position, velocity, rotation, inputs, suspension, tire conditions, reactor state, and more
- **Fabric-ready**: Includes complete Lakehouse ingestion notebook for Delta tables
- **Simple API**: Single `parse_gbx(filepath)` function
- **Extracts**:
  - Header metadata: player nickname, login, race time, map UID
  - Ghost info: start/end time, sample count, version
  - Telemetry samples: 52 fields at 50ms intervals (20Hz)

## Installation

### Basic Installation
```bash
pip install -e .
```

### Development Installation
```bash
pip install -e ".[dev]"
```

**Note**: For legacy replay `.Gbx` files with LZO-compressed body, `python-lzo>=1.14` can be optionally installed. New `.Ghost.Gbx` files use zlib compression (no LZO needed).

## Usage

### Basic Usage

```python
from tm_gbx import parse_gbx

# Parse a ghost file
result = parse_gbx('replay.Ghost.Gbx')

# Access metadata
print(f"Player: {result['metadata']['player_nickname']}")
print(f"Time: {result['metadata']['race_time_ms']} ms")
print(f"Map: {result['metadata']['map_uid']}")

# Access ghost info
print(f"Samples: {result['ghost_info']['num_samples']}")
print(f"Duration: {result['ghost_info']['start_time']}-{result['ghost_info']['end_time']} ms")

# Access ghost telemetry (52 fields per sample)
for sample in result['ghost_samples'][:10]:
    print(f"t={sample['time_ms']}ms: pos=({sample['x']:.2f}, {sample['y']:.2f}, {sample['z']:.2f}), "
          f"speed={sample['speed']:.2f}, steer={sample['steer']:.2f}, gas={sample['gas']:.2f}")
```

### Microsoft Fabric Lakehouse Ingestion

The parser includes a complete Fabric notebook for ingesting `.Ghost.Gbx` files into Delta tables. See `notebooks/fabric_ghost_ingest.py`.

**Input**: `.Ghost.Gbx` files in `/lakehouse/default/Files/ghosts/`

**Output**: Two Delta tables
- `ghost_header`: Metadata (one row per ghost file)
- `ghost_telemetry`: Telemetry samples (one row per sample, 52 fields)

```python
# The Fabric notebook handles:
# - Batch ingestion of multiple .Ghost.Gbx files
# - Parsing with inline logic (no pip install needed)
# - Creating properly-typed Spark DataFrames
# - Writing to Delta tables in append mode
# - Summary reporting
```

## Data Format

The `parse_gbx()` function returns a dictionary with:

```python
{
    'metadata': {
        'player_login': str,          # Player login ID
        'player_nickname': str,       # Player display name
        'map_uid': str,               # Map unique ID
        'race_time_ms': int,          # Race time in milliseconds
        'title_id': str,              # Game title ID
        # ... additional header fields
    },
    'ghost_info': {
        'start_time': int,            # Recording start time (ms)
        'end_time': int,              # Recording end time (ms)
        'num_samples': int,           # Number of telemetry samples
        'sample_period_ms': int,      # Time between samples (50ms)
        'version': int                # Record format version
    },
    'ghost_samples': [
        {
            # Time
            'time_ms': int,           # Sample timestamp (ms)
            'time_s': float,          # Sample timestamp (seconds)
            
            # Position & Velocity
            'x': float, 'y': float, 'z': float,  # Position (meters)
            'speed': float,           # Speed (game units, NOT km/h)
            'side_speed': float,      # Lateral speed
            'vel_x': float, 'vel_y': float, 'vel_z': float,  # Velocity vector
            
            # Rotation (Euler angles)
            'pitch_deg': float,       # Pitch (degrees)
            'yaw_deg': float,         # Yaw (degrees)
            'roll_deg': float,        # Roll (degrees)
            
            # Inputs
            'steer': float,           # Steering input [-1, 1]
            'gas': float,             # Gas pedal [0, 1]
            'brake': float,           # Brake pedal [0, 1]
            'gear': float,            # Current gear
            'rpm': int,               # Engine RPM (0-255)
            
            # Turbo & Reactor
            'is_turbo': bool,         # Turbo active
            'turbo_time': float,      # Turbo charge [0, 1]
            'reactor_state': int,     # Reactor state (0=off, 1=ground, 2=up, 3=down)
            'reactor_boost': int,     # Reactor boost level (0, 1, 2)
            'reactor_pedal': int,     # Reactor pedal (-1=brake, 0=none, 1=accel)
            'reactor_steer': int,     # Reactor steer (-1=left, 0=none, 1=right)
            
            # Contact & Simulation
            'is_ground_contact': bool,  # Wheels on ground
            'is_top_contact': bool,     # Roof contact
            'sim_time_coef': float,     # Simulation time coefficient
            'wetness': float,           # Surface wetness [0, 1]
            
            # Per-wheel Suspension (FL=Front-Left, FR=Front-Right, etc.)
            'fl_dampen': float, 'fr_dampen': float,  # Suspension compression
            'rr_dampen': float, 'rl_dampen': float,
            
            # Per-wheel Surface Conditions
            'fl_ice': float, 'fr_ice': float,        # Ice [0, 1]
            'rr_ice': float, 'rl_ice': float,
            'fl_dirt': float, 'fr_dirt': float,      # Dirt [0, 1]
            'rr_dirt': float, 'rl_dirt': float,
            'fl_slip': bool, 'fr_slip': bool,        # Wheel slip
            'rr_slip': bool, 'rl_slip': bool,
            'fl_ground_mat': int, 'fr_ground_mat': int,  # Surface material ID
            'rr_ground_mat': int, 'rl_ground_mat': int,
            
            # Per-wheel Rotation
            'fl_wheel_rot': float, 'fr_wheel_rot': float,  # Wheel rotation (radians)
            'rr_wheel_rot': float, 'rl_wheel_rot': float
        },
        # ... more samples (typically 500-1000 per ghost)
    ]
}
```

**Note**: The `speed` field is the game's native speed unit (`exp(i16/1000)`), NOT km/h. Convert to km/h with `speed_kmh = speed * 3.6` if needed.

## Architecture Notes

This parser is based on reverse-engineering the [gbx-net](https://github.com/BigBang1112/gbx-net) C# reference implementation, specifically:
- `CPlugEntRecordData.cs` (chunk 0x0911F000) - Ghost record container
- `CSceneVehicleVis.cs` (entity 0x0A018000) - Vehicle telemetry samples (107 bytes each)
- `GbxBodyReader.cs` - Binary reading and compression

### GBX File Structure

**For `.Ghost.Gbx` files:**
1. **Header**: Magic bytes (`GBX`), version, class ID, header chunks (metadata)
2. **Reference Table**: External node references (usually empty)
3. **Body**: **zlib-compressed** chunk data
4. **CPlugEntRecordData chunk (0x0911F000)**: Contains ghost record
   - Version (u32)
   - Inner data (zlib-compressed): EntRecordDescs, NoticeRecordDescs, Entity list
   - CSceneVehicleVis entity (0x0A018000): Contains 107-byte telemetry samples

**For legacy replay `.Gbx` files:**
- Body may use LZO compression instead of zlib (requires optional `python-lzo` package)
- Same CPlugEntRecordData structure inside the decompressed body

### Modules

- `tm_gbx.reader` - Binary reading primitives (uint8, int32, float, string)
- `tm_gbx.lookback` - GBX string interning system
- `tm_gbx.header` - Header and metadata chunk parsing
- `tm_gbx.ghost` - Ghost telemetry extraction (CPlugEntRecordData → CSceneVehicleVis)
- `tm_gbx.parser` - Main `parse_gbx()` entry point

## Testing

```bash
pytest tests/test_parser.py -v
```

Tests validate parsing on 8 real replay files in the `tests/` directory.

## Lakehouse Pipeline

This parser is part of a Microsoft Fabric Lakehouse pipeline for TrackMania 2020 ghost analytics:

- **Bronze Layer** (`tm2020_bronze`): Raw GBX file ingestion
- **Silver Layer** (`tm2020_silver`): Cleaned and normalized telemetry tables
- **Gold Layer** (`tm2020_gold`): Analytics-ready aggregated tables

See `notebooks/fabric_ghost_ingest.py` for the complete ingestion notebook that creates:
- `ghost_header` table: Metadata (one row per ghost)
- `ghost_telemetry` table: Telemetry samples (52 fields, millions of rows)

## Requirements

- Python 3.7+
- Optional: `python-lzo>=1.14` for legacy replay files (not needed for `.Ghost.Gbx` files)

## License

See LICENSE file for details.

## Credits

- [gbx-net](https://github.com/BigBang1112/gbx-net) - C# GBX parser reference (CPlugEntRecordData, CSceneVehicleVis)
- [pygbx](https://github.com/schadocalex/gbx.py) - Python GBX parser inspiration

