# TM2020 Fabric Lakehouse Architecture

## Overview

This document describes the Microsoft Fabric Lakehouse architecture for TrackMania 2020 replay analytics using the Medallion Architecture pattern (Bronze → Silver → Gold).

## Medallion Architecture

### Bronze Layer (`tm2020_bronze`)
**Purpose**: Raw data ingestion with minimal transformation

- **Source**: JSON files uploaded to Lakehouse Files (one file per replay)
- **Format**: Delta tables
- **Schema**: Direct mapping from JSON structure (based on `tm_gbx` output)
- **Tables**:
  - `bronze_replays_raw`: All replay data with metadata and ghost samples
  - Includes: player info, map info, race time, checkpoints, ghost telemetry
- **Characteristics**:
  - Preserves original data structure
  - No data quality rules applied
  - Append-only pattern
  - Includes ingestion timestamp

### Silver Layer (`tm2020_silver`)
**Purpose**: Cleaned, normalized, and conformed data

- **Source**: Bronze Delta tables
- **Format**: Delta tables
- **Tables**:
  - `silver_replays`: Cleaned replay metadata
  - `silver_ghost_samples`: Normalized telemetry samples
  - `silver_maps`: Map dimension table
  - `silver_players`: Player dimension table
- **Transformations**:
  - Data quality checks (null handling, type validation)
  - Deduplication
  - Normalization (dimensional modeling)
  - Standardized naming conventions
  - Filtering invalid/corrupted records

### Gold Layer (`tm2020_gold`)
**Purpose**: Business-ready aggregated datasets

- **Source**: Silver Delta tables
- **Format**: Delta tables
- **Tables**:
  - `gold_player_stats`: Aggregate player performance metrics
  - `gold_map_leaderboard`: Best times per map
  - `gold_race_analytics`: Race performance analysis
  - `gold_checkpoint_analysis`: Checkpoint-level insights
- **Aggregations**:
  - Best race times per player/map
  - Average speeds and consistency metrics
  - Checkpoint performance statistics
  - Player rankings and comparisons

## Data Model

### JSON Input Schema (from `tm_gbx` output)

```json
{
  "metadata": {
    "player_login": "string",
    "player_nickname": "string",
    "map_name": "string",
    "map_uid": "string",
    "map_author": "string",
    "race_time_ms": "integer",
    "checkpoints": ["integer"],
    "num_respawns": "integer",
    "game_version": "string",
    "title_id": "string"
  },
  "ghost_samples": [
    {
      "time_ms": "integer",
      "position": {"x": "float", "y": "float", "z": "float"},
      "velocity": {"x": "float", "y": "float", "z": "float"},
      "speed": "float"
    }
  ]
}
```

### Bronze Schema

**Table: `bronze_replays_raw`**
- `replay_id` (string): Unique identifier (derived from filename or hash)
- `ingestion_timestamp` (timestamp): When the record was ingested
- `metadata` (struct): Complete metadata object
- `ghost_samples` (array<struct>): Complete ghost samples array
- `source_file` (string): Original JSON filename

### Silver Schema

**Table: `silver_replays`**
- `replay_id` (string, PK)
- `player_login` (string)
- `player_nickname` (string)
- `map_uid` (string)
- `race_time_ms` (integer)
- `num_respawns` (integer)
- `num_checkpoints` (integer)
- `game_version` (string)
- `title_id` (string)
- `ingestion_date` (date)
- `is_valid` (boolean)

**Table: `silver_ghost_samples`**
- `sample_id` (string, PK): Generated unique ID
- `replay_id` (string, FK)
- `time_ms` (integer)
- `position_x` (float)
- `position_y` (float)
- `position_z` (float)
- `velocity_x` (float)
- `velocity_y` (float)
- `velocity_z` (float)
- `speed` (float)

**Table: `silver_maps`**
- `map_uid` (string, PK)
- `map_name` (string)
- `map_author` (string)
- `first_seen` (timestamp)
- `last_seen` (timestamp)

**Table: `silver_players`**
- `player_login` (string, PK)
- `player_nickname` (string)
- `first_seen` (timestamp)
- `last_seen` (timestamp)

### Gold Schema

**Table: `gold_player_stats`**
- `player_login` (string)
- `total_races` (integer)
- `total_maps_played` (integer)
- `avg_race_time_ms` (float)
- `best_race_time_ms` (integer)
- `total_respawns` (integer)
- `last_race_date` (date)

**Table: `gold_map_leaderboard`**
- `map_uid` (string)
- `map_name` (string)
- `player_login` (string)
- `player_nickname` (string)
- `best_time_ms` (integer)
- `rank` (integer)
- `race_date` (date)

**Table: `gold_race_analytics`**
- `replay_id` (string)
- `player_login` (string)
- `map_uid` (string)
- `race_time_ms` (integer)
- `avg_speed` (float)
- `max_speed` (float)
- `checkpoint_times` (array<integer>)
- `race_date` (date)

**Table: `gold_checkpoint_analysis`**
- `map_uid` (string)
- `checkpoint_number` (integer)
- `avg_time_ms` (float)
- `min_time_ms` (integer)
- `max_time_ms` (integer)
- `sample_count` (integer)

## Naming Conventions

### Tables
- **Bronze**: `bronze_<entity>_raw`
- **Silver**: `silver_<entity>` (singular for dimensions, plural for facts)
- **Gold**: `gold_<purpose>_<type>`

### Columns
- Use snake_case for all column names
- Use suffixes for clarity: `_ms` (milliseconds), `_id` (identifier), `_date`, `_timestamp`
- Boolean columns: `is_<condition>`, `has_<feature>`

### Files
- JSON files: `<replay_id>.json` or timestamp-based naming
- Notebooks: `<sequence>_<action>_<layer>.ipynb`

## Data Lineage

```
JSON Files (Lakehouse Files)
    ↓
[01_ingest_json_to_bronze.ipynb]
    ↓
bronze_replays_raw (Bronze Lakehouse)
    ↓
[02_bronze_to_silver.ipynb]
    ↓
silver_replays, silver_ghost_samples, silver_maps, silver_players (Silver Lakehouse)
    ↓
[03_silver_to_gold.ipynb]
    ↓
gold_player_stats, gold_map_leaderboard, gold_race_analytics, gold_checkpoint_analysis (Gold Lakehouse)
```

## Data Quality Rules

### Bronze Layer
- Accept all data as-is
- Track ingestion metadata
- No rejections at this layer

### Silver Layer
- **Mandatory fields**: player_login, map_uid, race_time_ms
- **Data type validation**: Ensure integers/floats are valid
- **Null handling**: Replace nulls with defaults or mark records as invalid
- **Deduplication**: Keep latest record per replay_id
- **Range validation**: race_time_ms > 0, speeds within reasonable bounds

### Gold Layer
- Aggregate only valid Silver records
- Apply business rules for rankings
- Handle edge cases (e.g., ties in leaderboard)

## Implementation Notes

### Performance Optimization
- Partition Bronze tables by ingestion_date
- Partition Silver tables by map_uid or player_login (depending on query patterns)
- Use Z-ordering on frequently filtered columns
- Optimize file sizes for Delta tables (target ~128MB per file)

### Incremental Processing
- Bronze: Append new JSON files since last run
- Silver: Process only new/updated Bronze records (using watermarks)
- Gold: Incremental aggregation where possible

### Error Handling
- Log errors to separate error tables
- Implement retry logic for transient failures
- Create data quality dashboards

## Future Enhancements
- Real-time ingestion using Event Hubs
- Machine learning models for race prediction
- Advanced analytics (trajectory optimization, racing line analysis)
- Integration with TrackMania API for live leaderboards
