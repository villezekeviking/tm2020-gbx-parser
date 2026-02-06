# TM2020 Fabric Lakehouse Pipeline

Microsoft Fabric Lakehouse pipeline for TrackMania 2020 replay analytics using Medallion Architecture (Bronze → Silver → Gold).

## Overview

This project implements a data engineering pipeline for analyzing TrackMania 2020 replay data using Microsoft Fabric's Lakehouse architecture. Pre-converted JSON files (from GBX replays) flow through three layers of transformation to produce analytics-ready datasets.

## Architecture

### Medallion Architecture Layers

**Bronze Layer** (`tm2020_bronze`)
- Raw JSON ingestion from Lakehouse Files
- Minimal transformation, preserves original structure
- Tables: `bronze_replays_raw`

**Silver Layer** (`tm2020_silver`)
- Cleaned and normalized tables
- Data quality rules applied
- Dimensional modeling
- Tables: `silver_replays`, `silver_ghost_samples`, `silver_maps`, `silver_players`

**Gold Layer** (`tm2020_gold`)
- Analytics-ready aggregated tables
- Business metrics and KPIs
- Tables: `gold_player_stats`, `gold_map_leaderboard`, `gold_race_analytics`, `gold_checkpoint_analysis`

See [Architecture Documentation](docs/architecture.md) for detailed information.

## Prerequisites

- Microsoft Fabric workspace
- Python 3.10+
- Three Lakehouses created: `tm2020_bronze`, `tm2020_silver`, `tm2020_gold`

## Data Flow

```
┌─────────────────┐
│  Local Machine  │
└────────┬────────┘
         │
         ├─ 1. Download GBX files (PowerShell scripts)
         │
         ├─ 2. Convert GBX → JSON (tm_gbx package)
         │     One JSON file per replay
         │
         ├─ 3. Manual upload to Bronze Lakehouse Files
         │     Location: /Files/replays/*.json
         │
┌────────▼────────────────────────────────────────────┐
│             Microsoft Fabric Workspace              │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌───────────────────────────────────────────┐    │
│  │  Bronze: 01_ingest_json_to_bronze.ipynb   │    │
│  │  - Read JSON from Files                   │    │
│  │  - Write to bronze_replays_raw            │    │
│  └──────────────────┬────────────────────────┘    │
│                     │                              │
│  ┌──────────────────▼────────────────────────┐    │
│  │  Silver: 02_bronze_to_silver.ipynb        │    │
│  │  - Clean & validate data                  │    │
│  │  - Normalize to fact/dimension tables     │    │
│  │  - Write to silver_* tables               │    │
│  └──────────────────┬────────────────────────┘    │
│                     │                              │
│  ┌──────────────────▼────────────────────────┐    │
│  │  Gold: 03_silver_to_gold.ipynb            │    │
│  │  - Aggregate metrics                      │    │
│  │  - Create analytics tables                │    │
│  │  - Write to gold_* tables                 │    │
│  └───────────────────────────────────────────┘    │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## Lakehouse Setup

### 1. Create Lakehouses
In your Microsoft Fabric workspace, create three Lakehouses:
- `tm2020_bronze`
- `tm2020_silver`
- `tm2020_gold`

### 2. Attach Notebooks
- Bronze notebook → attach to `tm2020_bronze`
- Silver notebook → attach to both `tm2020_silver` and `tm2020_bronze`
- Gold notebook → attach to both `tm2020_gold` and `tm2020_silver`

## Usage

### Step 1: Local Data Preparation

```bash
# 1. Download GBX replay files (scripts coming in PR #5)
# PowerShell scripts will be available in scripts/local/

# 2. Convert GBX to JSON using tm_gbx package
pip install -e .

python -c "
from tm_gbx import GBXParser
import json

parser = GBXParser('replay.Gbx')
data = parser.parse()

with open('replay.json', 'w') as f:
    json.dump(data, f, indent=2)
"
```

See [Local Scripts README](scripts/local/README.md) for detailed conversion instructions.

### Step 2: Upload to Fabric

1. Open `tm2020_bronze` Lakehouse in Fabric
2. Navigate to **Files** section
3. Create folder: `replays`
4. Upload JSON files to `Files/replays/`

### Step 3: Run Notebooks

Execute notebooks in order:

1. **Bronze**: `notebooks/bronze/01_ingest_json_to_bronze.ipynb`
   - Ingests JSON files to Bronze Delta tables
   
2. **Silver**: `notebooks/silver/02_bronze_to_silver.ipynb`
   - Transforms Bronze → Silver with data quality checks
   
3. **Gold**: `notebooks/gold/03_silver_to_gold.ipynb`
   - Aggregates Silver → Gold analytics tables

## JSON Data Format

Input JSON files should match the `tm_gbx` parser output format:

```json
{
  "metadata": {
    "player_login": "string",
    "player_nickname": "string",
    "map_name": "string",
    "map_uid": "string",
    "map_author": "string",
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

## Output Tables

### Bronze
- `bronze_replays_raw`: Raw JSON data with ingestion metadata

### Silver
- `silver_replays`: Cleaned replay metadata
- `silver_ghost_samples`: Normalized telemetry samples
- `silver_maps`: Map dimension table
- `silver_players`: Player dimension table

### Gold
- `gold_player_stats`: Player performance metrics
- `gold_map_leaderboard`: Best times per map with rankings
- `gold_race_analytics`: Race performance analysis
- `gold_checkpoint_analysis`: Checkpoint-level insights

## Project Structure

```
.
├── notebooks/
│   ├── bronze/
│   │   └── 01_ingest_json_to_bronze.ipynb
│   ├── silver/
│   │   └── 02_bronze_to_silver.ipynb
│   └── gold/
│       └── 03_silver_to_gold.ipynb
├── docs/
│   └── architecture.md
├── scripts/
│   └── local/
│       └── README.md
├── tm_gbx/                    # Legacy GBX parser (kept for reference)
│   ├── parser.py
│   ├── models.py
│   └── ...
├── README.md
└── requirements.txt
```

## Legacy Code

The `tm_gbx` directory contains the original GBX parser Python library. This code is **kept for reference** and will be removed in a future release. It is still useful for:
- Understanding the JSON data structure
- Local GBX → JSON conversion
- Reference implementation

## Development

### Dependencies
```bash
pip install -r requirements.txt
```

### Running Locally (for testing)
The notebooks are designed for Microsoft Fabric but can be adapted for local Spark environments for development purposes.

## Roadmap

- [x] Medallion architecture implementation
- [x] Bronze layer ingestion
- [x] Silver layer transformation
- [x] Gold layer aggregation
- [ ] PowerShell download scripts (PR #5)
- [ ] Incremental processing optimization
- [ ] Data quality dashboards
- [ ] Real-time ingestion via Event Hubs
- [ ] Machine learning models for race prediction

## Contributing

This is a data engineering project for TrackMania 2020 analytics. Contributions are welcome for:
- Performance optimizations
- Additional analytics metrics
- Data quality improvements
- Documentation enhancements

## License

See LICENSE file for details.

## Credits

Based on:
- [GBX.NET](https://github.com/BigBang1112/gbx-net) - C# GBX parser
- [pygbx](https://github.com/schadocalex/gbx.py) - Python GBX parser for older TM versions

