# Local Scripts

## Purpose
This directory contains local utility scripts for TrackMania 2020 GBX file download and conversion. These scripts are intended to run on a local machine, not in Microsoft Fabric.

## Workflow
The local scripts support the following workflow:

1. **Download GBX files**: PowerShell scripts to download TrackMania 2020 replay files (.Gbx) from various sources
2. **Convert GBX to JSON**: Use the `tm_gbx` Python package to convert binary GBX files to JSON format
3. **Upload to Lakehouse**: Manually upload the generated JSON files to the Bronze Lakehouse Files area in Microsoft Fabric

## Upcoming Scripts (PR #5)
The following PowerShell scripts will be added in a future pull request:

- `Download-TMReplays.ps1`: Download replay files from TrackMania servers
- `Download-MapReplays.ps1`: Download replays for specific maps
- `Batch-Convert.ps1`: Batch convert multiple GBX files to JSON

## GBX to JSON Conversion

### Using the tm_gbx Package
The existing `tm_gbx` Python package in this repository can be used to convert GBX replay files to JSON format.

#### Installation
```bash
# Install from the repository root
pip install -e .
```

#### Usage
```python
from tm_gbx import GBXParser
import json

# Parse a GBX file
parser = GBXParser('replay.Gbx')
data = parser.parse()

# Convert to JSON
with open('replay.json', 'w') as f:
    json.dump(data, f, indent=2)
```

#### Batch Conversion Example
```python
import os
import json
from pathlib import Path
from tm_gbx import GBXParser

# Convert all GBX files in a directory
gbx_dir = Path('replays/gbx')
json_dir = Path('replays/json')
json_dir.mkdir(exist_ok=True)

for gbx_file in gbx_dir.glob('*.Gbx'):
    try:
        parser = GBXParser(str(gbx_file))
        data = parser.parse()
        
        # Save as JSON
        json_file = json_dir / f"{gbx_file.stem}.json"
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Converted: {gbx_file.name} -> {json_file.name}")
    except Exception as e:
        print(f"Error converting {gbx_file.name}: {e}")
```

### Output Format
The JSON files will have the following structure (based on `tm_gbx/models.py`):

```json
{
  "metadata": {
    "player_login": "player_login_id",
    "player_nickname": "Player Name",
    "map_name": "Map Name",
    "map_uid": "unique_map_identifier",
    "map_author": "Map Author",
    "race_time_ms": 45230,
    "checkpoints": [12500, 25000, 37500],
    "num_respawns": 0,
    "game_version": "TM2020",
    "title_id": "Trackmania"
  },
  "ghost_samples": [
    {
      "time_ms": 0,
      "position": {"x": 0.0, "y": 0.0, "z": 0.0},
      "velocity": {"x": 0.0, "y": 0.0, "z": 0.0},
      "speed": 0.0
    }
  ]
}
```

## File Naming Convention
For consistency with the Bronze ingestion notebook, use the following naming convention for JSON files:

- `<map_uid>_<player_login>_<timestamp>.json`
- Or simply: `replay_<unique_id>.json`

Example: `T3YxMNpRvH4PL5k7_player123_20240115_143052.json`

## Upload to Microsoft Fabric

### Manual Upload Steps
1. Navigate to your Microsoft Fabric workspace
2. Open the `tm2020_bronze` Lakehouse
3. Go to the **Files** section
4. Create a folder named `replays` (if it doesn't exist)
5. Upload the JSON files to `Files/replays/`
6. Run the Bronze ingestion notebook (`notebooks/bronze/01_ingest_json_to_bronze.ipynb`)

### Automated Upload (Future Enhancement)
Future enhancements may include:
- Azure Storage Explorer upload scripts
- OneLake API integration for programmatic upload
- Azure Data Factory pipelines for automated file transfer

## Directory Structure
```
scripts/
  local/
    README.md                    # This file
    (PowerShell scripts coming in PR #5)
```

## Prerequisites
- Python 3.10+
- `tm_gbx` package (install from repository root)
- PowerShell 7+ (for download scripts)
- Microsoft Fabric workspace access (for upload)

## Notes
- GBX files are binary and should not be committed to Git (already in `.gitignore`)
- JSON files are also ignored by Git to avoid repository bloat
- Keep local scripts in this directory for organization
- Conversion logic references the existing `tm_gbx` package

## Related Documentation
- [Architecture Documentation](../../docs/architecture.md) - Medallion architecture overview
- [Bronze Notebook](../../notebooks/bronze/01_ingest_json_to_bronze.ipynb) - JSON ingestion logic
- [tm_gbx Package](../../tm_gbx/) - GBX parser implementation
