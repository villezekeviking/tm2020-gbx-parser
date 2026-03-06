# Leaderboard .Replay.Gbx Reverse Engineering Notes

> Session date: 2026-03-06
> Status: **In Progress** — 3 of 5 test files fully parsing, 2 files have decompression issues being investigated

## Problem Statement

The existing `fabric_ghost_ingest.py` notebook could parse `.Ghost.Gbx` files (class `0x03093000`) but failed on **leaderboard `.Replay.Gbx` files** (class `0x03092000`) downloaded via the Nadeo API. These leaderboard replays use a completely different binary format.

## Key Differences: Ghost.Gbx vs Leaderboard Replay.Gbx

| Property | .Ghost.Gbx (0x03093000) | Leaderboard .Replay.Gbx (0x03092000) |
|---|---|---|
| Class ID | `0x03093000` | `0x03092000` |
| User data / header metadata | Yes (map_uid, player, race_time) | **None** (user_data_size = 0) |
| Body compression | Single zlib stream for entire body | **Per-chunk** compression |
| Body compression flag | `0x43` = compressed | `0x43` = compressed |
| Record version | Typically ≤ 10 | **11** |
| CPlugEntRecordData inner zlib | Standard `zlib.decompress()` | **Raw deflate** (`wbits=-15`, skip 2-byte `78 9c` header) |
| Sample storage | Per-sample tagged: `has_sample(u8) + time(i32) + length(u32) + data` | **Column-oriented, delta-encoded** |
| Sample size | 107 bytes (CSceneVehicleVis) | 116 bytes (107 compatible + 9 extra trailing bytes) |
| Entity descriptor sampleSize field | Matches actual stored size | States `864` (but actual stored size per key-column is 1 byte per sample) |
| Sample period | Embedded per-sample timestamps | Fixed **50ms** period with delta array |

## GBX Header Layout (All 5 Test Files Identical)

```
Offset  Bytes           Meaning
0       47 42 58        Magic "GBX"
3       06 00           Version 6
5       42              Format (binary)
6       55              Ref table compression
7       43              Body compression (0x43 = compressed)
8       52              Unknown byte (version >= 6)
9       00 20 09 03     Class ID: 0x03092000
13      00 00 00 00     User data size: 0
17      ...             (ref table + body start)
```

## File Structure After Header

```
Offset 17:  num_external_refs (u32) = 2
Offset 21:  ancestor_level (u32) = 0
Offset 25:  body_uncomp_size (u32)   ← size of uncompressed body
Offset 29:  body_comp_size (u32)     ← size of compressed body
Offset 33:  body data starts         ← NOT a single zlib stream; per-chunk compressed
```

The body is a sequence of GBX chunks, each independently compressed. The body is NOT decompressible as a single zlib stream.

## Finding CPlugEntRecordData

Search for byte pattern `\x00\xf0\x11\x09` (chunk ID `0x0911F000` in little-endian) in the raw file data (after the 17-byte header).

**Important:** The pattern appears TWICE — first in the chunk list, then as the actual chunk. If the u32 immediately after the pattern is also `0x0911F000`, skip to the next occurrence.

```python
remaining = file_data[17:]
pattern = b'\x00\xf0\x11\x09'
idx = remaining.find(pattern)
# Skip doubled chunk-list entry
if struct.unpack_from('<I', remaining, idx + 4)[0] == 0x0911F000:
    idx = remaining.find(pattern, idx + 4)
```

## CPlugEntRecordData Chunk Layout

```
+0:   version (u32) = 11
+4:   uncompressed_size (u32)
+8:   compressed_size (u32)
+12:  zlib data (starts with 78 9c header)
```

### Decompression

Standard `zlib.decompress()` fails with "incorrect data check". Must use **raw deflate** — skip the 2-byte zlib header (`78 9c`) and use `wbits=-15`:

```python
d = zlib.decompressobj(-15)
record_data = d.decompress(compressed[2:])  # skip 78 9c
record_data += d.flush()
```

**Note:** For some files (e.g., pos004), standard `zlib.decompress()` works fine. The raw deflate approach works for all files that decompress at all.

## Decompressed Record Data Layout (Version 11)

```
+0:   start_time (i32) = 0
+4:   end_time (i32)          e.g., 54250 ms

Entity Descriptors:
+8:   ent_count (u32) = 7
      For each descriptor:
        class_id (u32)        e.g., 0x0A018000 = CSceneVehicleVis
        sample_size (i32)     e.g., 864 (NOT the actual stored sample byte count)
        unknown1 (i32)
        unknown2 (i32)
        desc_data_len (u32)
        desc_data (bytes)
        unknown3 (i32)

Notices:
        notice_count (u32)    e.g., 82
        For each: 12 bytes (3 × i32)

Entity Records:
        has_entity (u8) = 1 or 0
        entity_idx (i32)      ← INDEX into descriptor table (not the class ID!)
        ... (see below)
```

### Entity Descriptors Found (All 5 Files)

| Index | Class ID | Name | sampleSize |
|-------|----------|------|------------|
| 0 | 0x0A019000 | ? | 588 |
| 1 | 0x2F0CB000 | ? | 4 |
| 2 | 0x0A018000 | **CSceneVehicleVis** | 864 |
| 3 | 0x032E3000 | ? | 68 |
| 4 | 0x032AC000 | ? | 48 |
| 5 | 0x2D001000 | ? | 2156 |
| 6 | 0x032CB000 | ? | 40 |

## Version 11 Entity Record Format (Column-Oriented)

This is the **critical discovery**. Unlike earlier versions where each sample is individually tagged, v11 stores data in a columnar/delta format:

```
Entity Header:
  has_entity (u8)         = 1
  entity_idx (i32)        = 2 (index into descriptor table → CSceneVehicleVis)
  flags (u32)             = 0x02000006
  start_offset (i32)      = 0
  end_offset (i32)        = 54250 (matches end_time)
  unknown (i32)           = 0
  num_samples (u32)       = 1086
  num_keys (u32)          = 116
  unknown2 (u32)          = 0

Time Deltas:
  (num_samples - 1) × u32 values, all = 50 (ms per sample)

Sample Data (column-oriented):
  num_keys columns × num_samples bytes each
  Total = num_keys × num_samples bytes
```

### Column Layout

The sample data is stored **column-oriented** (also called "struct of arrays"):

```
Column 0: [sample0_key0, sample1_key0, sample2_key0, ..., sample1085_key0]  (1086 bytes)
Column 1: [sample0_key1, sample1_key1, sample2_key1, ..., sample1085_key1]  (1086 bytes)
...
Column 115: [sample0_key115, ..., sample1085_key115]                          (1086 bytes)
```

### Delta Encoding

Each column is **delta-encoded**:
- First value (index 0) is **absolute**
- All subsequent values are **signed byte deltas** from the previous value

```python
# Delta-decode a column
for i in range(1, len(col)):
    delta = struct.unpack('b', bytes([col[i]]))[0]  # signed byte
    col[i] = (col[i-1] + delta) & 0xFF
```

### Sample Reconstruction

To get sample N, take byte N from each decoded column:

```python
sample = bytes([decoded_columns[k][N] for k in range(num_keys)])
```

The resulting 116-byte sample is **compatible with the existing CSceneVehicleVis parser** — the first 107 bytes use the same field layout (position at offset 47, speed at 65, steer at 14, etc.).

## Verified Telemetry Output

Sample output from pos001 (race time 54.255s):

```
    Time          X        Y          Z    Speed  Steer   Gas
       0ms    1513.30     2.00    1063.18     0.3   0.12  1.00
    5000ms    1447.03    14.47     983.40    22.9  -0.04  1.00
   25000ms    1180.10   129.83    1237.30    28.5   0.47  1.00
   50000ms     900.68   252.03    1163.97    25.9   0.08  1.00
   54250ms     996.03   257.87    1093.00     —      —     —
```

- ✅ Position smoothly changes (hillclimb map — Y goes from 2m to 257m)
- ✅ Speed ranges 0–51 km/h (reasonable for hillclimb)
- ✅ Steering alternates left/right
- ✅ Duration matches race time (54.250s ≈ 54.255s)

## Test File Status

| File | Size | Time | Samples | Status | Notes |
|------|------|------|---------|--------|-------|
| pos001_54255 | 42,649 | 54,250ms | 1,086 | ✅ Works | Raw deflate needed |
| pos002_54843 | 61,373 | ? | ? | ❌ Decompression fails | Gets 85,292 bytes (18%) then "invalid stored block lengths" at 32KB boundary |
| pos003_54927 | 87,148 | ? | ? | ❌ Decompression fails | Fails immediately with "invalid distance too far back" |
| pos004_54930 | 43,920 | 54,930ms | 1,099 | ✅ Works | Both standard zlib and raw deflate work |
| pos005_54973 | 39,262 | 54,970ms | 1,100 | ✅ Works | Raw deflate needed; had `list index out of range` in v2 (fixed in v3 with multi-entity loop) |

### Decompression Issue Hypothesis (pos002/pos003)

- pos002 partially decompresses (85KB out of 466KB expected) then fails at a 32KB boundary — suggests the data may be split into **multiple compressed segments** (LZO chunks or windowed zlib)
- pos003 fails immediately — the zlib stream at offset 180 may not be the CPlugEntRecordData inner data at all
- Both files are **larger** than the working files (61KB and 87KB vs ~40KB), suggesting more complex/longer replays
- All files have `body_comp=0x43` and the body is structured as **individual GBX chunks** (starting with chunk IDs like `0x03F00637`), not a single compressed stream
- **Next step:** Parse the body as individual chunks and find the CPlugEntRecordData chunk within the properly-decompressed outer body

## Current Investigation: Per-Chunk Body Parsing

The body at offset 33 starts with GBX chunk IDs (e.g., `0x03F00637`). The structure should be:

```
chunk_id (u32) + chunk_size (u32) [top bit = "heavy"/compressed flag] + chunk_data
```

This is the next script to run (debug_chunks.py). The hypothesis is:
1. Parse the body as individual chunks
2. Some chunks may need LZO decompression (not zlib)
3. The CPlugEntRecordData chunk data will be found within the properly parsed chunk sequence
4. Once found, the inner zlib (raw deflate) + column-oriented delta decoding should work identically

## Working Parser Code (v3)

The current working parser (`fabric_ghost_ingest_v3.py`) handles:
- ✅ Class `0x03092000` (leaderboard replays) — searches raw file for CPlugEntRecordData, raw deflate decompression, v11 column-oriented delta decoding
- ✅ Class `0x03093000` (player ghosts) — standard zlib body, per-sample tagged format
- ✅ Multi-entity support — loops through all entities to find CSceneVehicleVis
- ⚠️ Still needs fix for pos002/pos003 decompression

## Debug Scripts Reference

| Script | Purpose | Key Finding |
|--------|---------|-------------|
| debug_replay_v1 | Identify class ID | Class is `0x03092000`, not `0x03093000` |
| debug_replay_v2 | Identify why parsing fails | user_data_size=0, body_comp=0x43, no header metadata |
| debug_replay_v3 | Find CPlugEntRecordData | Found at offset +287 via byte pattern search |
| debug_replay_v4 | Identify version and sizes | Record version=11, uncomp=157344, comp=37317 |
| debug_replay_v5 | Brute-force decompression | Raw deflate (`wbits=-15`) works! Got 151,685 bytes |
| debug_replay_v7 | Correct deflate offset | Skip 2-byte `78 9c` header, exact comp_size - 2 bytes |
| debug_replay_v8 | Parse entity records | Found CSceneVehicleVis (entity_idx=2), but 0 samples with old parser |
| debug_replay_v9 | Examine entity layout | Discovered num_samples=1086, num_keys=116, repeating 50ms period |
| debug_replay_v12 | Count time deltas | 1085 deltas of 50ms (+ 1 implicit = 1086 samples) |
| debug_replay_v13 | Find sample data | Data after deltas is column-oriented, not row-oriented |
| debug_replay_v14 | Understand column layout | 116 columns × 1086 bytes each = 125,976 bytes. Next entity at boundary |
| debug_replay_v15 | Column-oriented decode | Transposed columns show position at offset 47 for sample 0 only |
| debug_replay_v16 | Delta-decode columns | **BREAKTHROUGH:** Delta decoding gives perfect telemetry for all 1086 samples |
| debug_stuck | Diagnose v3 hanging | Garbage entity[1] with 4B samples caused infinite allocation loop |
| debug_decompress | Why pos002/003 fail | Both fail zlib AND raw deflate; pos002 gets 18% before failing |
| debug_body_decompress | Find outer body | Body not a single zlib stream; starts with GBX chunk IDs |
| debug_chunks | Per-chunk parsing | **Currently running** |

## Architecture Decision

The parser uses a **two-path approach** in `parse_gbx_file()`:

```python
def parse_gbx_file(filepath):
    class_id = struct.unpack_from('<I', file_data, 9)[0]
    if class_id == 0x03092000:
        return _parse_replay_v11(file_data)   # leaderboard replay
    return _parse_ghost_gbx(file_data)         # player ghost
```

For leaderboard replays, the CPlugEntRecordData is found by scanning raw file bytes for the chunk signature, rather than parsing the full GBX chunk tree. This works for 3/5 files. To fix the remaining 2, we likely need to properly parse the per-chunk body structure.