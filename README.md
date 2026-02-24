# TM2020 GBX Parser

Pure-Python parser for TrackMania 2020 GBX replay files. Designed to work in Microsoft Fabric Notebooks (PySpark) and standard Python environments.

## Features

- **Pure Python**: No external dependencies required (uses only Python stdlib)
- **Optional LZO support**: Install `python-lzo` for full ghost sample extraction
- **Fabric-ready**: Works seamlessly in Microsoft Fabric Notebooks
- **Simple API**: Single `parse_gbx(filepath)` function
- **Extracts**:
  - Player nickname and login
  - Race time in milliseconds
  - Map UID and map info
  - Ghost samples (position data over time)

## Installation

### Basic Installation (Header Metadata Only)
```bash
pip install -e .
```

### Full Installation (With Ghost Samples)
```bash
pip install -e ".[lzo]"
```

### Development Installation
```bash
pip install -e ".[dev,lzo]"
```

## Usage

### Basic Usage

```python
from tm_gbx import parse_gbx

# Parse a replay file
result = parse_gbx('replay.Gbx')

# Access metadata
print(f"Player: {result['metadata']['player_nickname']}")
print(f"Time: {result['metadata']['race_time_ms']} ms")
print(f"Map: {result['metadata']['map_uid']}")

# Access ghost samples (if python-lzo installed)
for sample in result['ghost_samples'][:10]:
    print(f"t={sample['time_ms']}ms: "
          f"pos=({sample['position']['x']:.2f}, "
          f"{sample['position']['y']:.2f}, "
          f"{sample['position']['z']:.2f})")
```

### Microsoft Fabric Notebook Usage

The parser is designed to work in Fabric Notebooks with PySpark. See `notebooks/fabric_test.py` for a complete example.

```python
from tm_gbx import parse_gbx

# Parse replay file
result = parse_gbx('path/to/replay.Gbx')

# Create Spark DataFrame from ghost samples
# Note: 'spark' session is auto-available in Fabric
flattened = []
for sample in result['ghost_samples']:
    flattened.append({
        'time_ms': sample['time_ms'],
        'pos_x': sample['position']['x'],
        'pos_y': sample['position']['y'],
        'pos_z': sample['position']['z']
    })

df = spark.createDataFrame(flattened)
df.show()
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
        # ... additional fields
    },
    'ghost_info': {
        'is_fixed_timestep': bool,
        'sample_period': int,         # Time between samples in ms
        'version': int,
        'num_samples': int
    },
    'ghost_samples': [
        {
            'time_ms': int,           # Sample timestamp
            'position': {
                'x': float,           # X coordinate
                'y': float,           # Y coordinate
                'z': float            # Z coordinate
            }
        },
        # ... more samples
    ]
}
```

## Architecture Notes

This parser is based on studying the [gbx-net](https://github.com/BigBang1112/gbx-net) C# reference implementation, specifically:
- `CGameCtnReplayRecord.cs` - Replay record structure
- `CGameGhost.Data.cs` - Ghost data format
- `CSceneVehicleCar.Sample.cs` - Vehicle sample structure
- `GbxHeaderReader.cs`, `GbxBodyReader.cs` - Binary reading
- `CompressedData.cs` - Compression handling

### GBX File Structure

1. **Header**: Magic bytes, version, class ID, header chunks (metadata)
2. **Reference Table**: External node references (usually empty for TM2020)
3. **Body**: LZO-compressed chunk data containing ghost samples (zlib-compressed inside)

### Modules

- `tm_gbx.reader` - Binary reading primitives (uint8, int32, float, vec3, string)
- `tm_gbx.lookback` - GBX string interning system
- `tm_gbx.header` - Header and metadata chunk parsing
- `tm_gbx.ghost` - Ghost sample extraction from body
- `tm_gbx.parser` - Main `parse_gbx()` entry point

## Testing

```bash
pytest tests/test_parser.py -v
```

Tests validate parsing on 8 real replay files in the `tests/` directory.

## Lakehouse Pipeline

This parser is part of a larger Microsoft Fabric Lakehouse pipeline for TrackMania 2020 replay analytics:

- **Bronze Layer** (`tm2020_bronze`): Raw JSON ingestion
- **Silver Layer** (`tm2020_silver`): Cleaned and normalized tables
- **Gold Layer** (`tm2020_gold`): Analytics-ready aggregated tables

See notebooks in `notebooks/bronze/`, `notebooks/silver/`, and `notebooks/gold/` for the full pipeline.

## Requirements

- Python 3.7+
- Optional: `python-lzo>=1.14` for ghost sample extraction

## License

See LICENSE file for details.

## Credits

- [gbx-net](https://github.com/BigBang1112/gbx-net) - C# GBX parser reference
- [pygbx](https://github.com/schadocalex/gbx.py) - Python GBX parser inspiration

