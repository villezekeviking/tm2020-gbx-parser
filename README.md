# TM2020 GBX Parser

Python parser for TrackMania 2020 GBX replay files.

## Installation

```bash
pip install python-lzo
pip install -e .
```

## Usage

```python
from tm_gbx import GBXParser

# Parse a replay file
parser = GBXParser('replay.Gbx')
data = parser.parse()

# Access metadata
print(f"Player: {data['metadata']['player_nickname']}")
print(f"Time: {data['metadata']['race_time_ms']}ms")
print(f"Checkpoints: {data['metadata']['checkpoints']}")

# Access ghost samples (if available)
for sample in data['ghost_samples'][:10]:
    print(f"Time {sample['time_ms']}ms: Position {sample['position']}")
```

## Output Format

Returns a dictionary with:
- `metadata`: Player info, map info, race time, checkpoints
- `ghost_samples`: Position, velocity, speed at each time step

## Limitations

Currently extracts metadata and checkpoint times reliably. Full telemetry (position, velocity, inputs every 50ms) extraction is in development.

## Credits

Based on:
- [GBX.NET](https://github.com/BigBang1112/gbx-net) - C# GBX parser
- [pygbx](https://github.com/schadocalex/gbx.py) - Python GBX parser for older TM versions
