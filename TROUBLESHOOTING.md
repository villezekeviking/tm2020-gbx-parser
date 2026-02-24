# TM2020 GBX Ghost Data Parsing — Research Notes

> **Purpose**: This document captures all findings from reverse-engineering the ghost/replay data
> in a TM2020 `.Gbx` replay file. If starting a new chat, read this file first to continue where we left off.

## File Under Analysis
- **File**: `Ville (Best).Gbx` (TM2020 replay)
- **Race time**: 26,335 ms
- **Expected sample count**: ~527 (at 50ms intervals) or ~2,634 (at 10ms intervals)

---

## 1. High-Level File Structure (CONFIRMED)

```
GBX Header
  └─ body_compressed = 0x43 (67) → LZO compressed
  └─ LZO body: 1,670,407 bytes → decompresses to 1,708,063 bytes ✓
```

- **No external references** (num_external = 0)
- LZO decompression works perfectly using `lzo.decompress(data, False, uncompressed_size)`

---

## 2. Zlib Chunks Inside the LZO Body

Three valid zlib streams found inside the decompressed body:

| Offset    | Header | Decompressed Size | Contents                        |
|-----------|--------|-------------------|---------------------------------|
| 1,508,892 | 78da   | **186,943 bytes** | `CPlugEntRecordData` (chunk IDs `0x0602200B`–`0x0602201A`) |
| 1,597,349 | 78da   | 96,512 bytes      | Unknown (no position floats found) |
| 1,673,701 | 789c   | **82,500 bytes**  | `CGameCtnGhost` checkpoint/event data |

### How to decompress:
```python
import zlib
dec = zlib.decompressobj()
result = dec.decompress(body_data[offset:offset + 2_000_000])
result += dec.flush()
```

**IMPORTANT**: The zlib chunks do NOT have preceding size fields at the expected positions.
You must use streaming `zlib.decompressobj()` — you cannot rely on reading `(uncompressed_size, compressed_size)` from bytes before the zlib header.

---

## 3. Class IDs Found

### In the full decompressed body (NOT inside zlib chunks):
| Class ID     | Name                        | Offset    |
|--------------|-----------------------------|-----------|
| `0x03092000` | CGameCtnGhost               | 1,673,373 |
| `0x0303F006` | CGameGhost (chunk 006)      | 1,673,377 |
| `0x03092005` | CGameCtnGhost (chunk 005)   | 1,705,363 |
| `0x03092008` | CGameCtnGhost (chunk 008)   | 1,705,379 |
| `0x0309200A` | CGameCtnGhost (chunk 00A)   | 1,705,395 |
| `0x0309200B` | CGameCtnGhost (chunk 00B)   | 1,705,411 |

### NOT found anywhere:
- `0x0A02B000` — `CSceneVehicleCar` (used in older TM games, NOT in TM2020)
- `0x0A018000` — `CSceneVehicleVis` (used in gbx-net's delta parser, not found as raw ID)

**Key finding**: TM2020 does NOT use `CSceneVehicleCar` (`0x0A02B000`) for ghost data.
The original parser's `find_zlib_ghost_data()` function searches for this pattern and will always fail.

---

## 4. CPlugEntRecordData (186k Zlib Chunk) — The Ghost Samples

### Chunk Structure
Inside the 186k zlib chunk, the data is organized as GBX chunks with `PIKS` skip markers:

| Offset | Chunk ID     | Size (bytes) |
|--------|-------------|-------------|
| 0      | `0x0602200B` | 8           |
| 20     | `0x0602200F` | 8           |
| 40     | `0x06022013` | 16          |
| 68     | `0x06022015` | 60          |
| 140    | `0x06022016` | 4           |
| 156    | `0x06022017` | 8           |
| 176    | `0x06022018` | 8           |
| 196    | `0x06022019` | 4           |
| **212**| **`0x0602201A`** | **165,851** |

**Chunk `0x0602201A`** (165,851 bytes) contains the actual sample recording data.

### Chunk 0x0602201A Header (offset 224 in the decompressed zlib data)

```
Offset  Value       Interpretation (UNCERTAIN — still being decoded)
[  0]   13          Possibly num_entities or version
[  4]   1           Unknown (version?)
[ 8-24] 0,0,0,0,0  Zeros
[ 28]   256         Unknown (0x100, possibly flags)
[ 32]   64          Likely sample_size in bytes
[ 36]   25          Possibly num_keys in key table
[ 40+]  Small ints  Key/descriptor table entries
[ 72]   20808       0x5148 — appears again at offset 136 as 0x51480000
[ 76]   0xFF7FFFFF  -FLT_MAX sentinel
[80-92] 2.2, 2.2, 2.0, 1.0  Float default values (descriptor defaults?)
```

### Data Format (from gbx-net source `CPlugEntRecordData.cs`)

The actual sample data uses **`ReadEncodedDeltas`** (line 246 in gbx-net):

1. `numSamples` (i32)
2. `sampleSize` (i32) — bytes per sample
3. **Delta times**: `numSamples` × i32 (delta time per sample)
4. **Column-wise delta-accumulated data**:
   - For each byte column `i` (0 to sampleSize-1):
     - Read `numSamples` bytes
     - Delta-accumulate: `accumulator = (accumulator + byte) & 0xFF`
     - Store `accumulator` as `samples[row][col]`

**This means the sample data is TRANSPOSED and DELTA-ENCODED.**
You cannot simply read consecutive bytes as float samples.

### Outer Structure (from gbx-net `ReadEntList`, line 196)

The full parsing flow for the decompressed record data:
1. Read `start` time (TimeInt32)
2. Read `end` time (TimeInt32)
3. Read `EntRecordDescs[]` array (defines entity types + classIds)
4. Read `NoticeRecordDescs[]` (if version ≥ 1)
5. For each entity in `EntList`:
   - Read `type` (u32) → index into EntRecordDescs
   - Read samples via `ReadEncodedDeltas()` or `ReadEntRecordDeltas()`
   - Read `Samples2` (if version ≥ 2)
6. Read `BulkNoticeList` (if version ≥ 1)

### PROBLEM: We haven't correctly identified where `ReadEncodedDeltas` starts

Our attempt to read `(numSamples=256, sampleSize=64)` at offset 28 produced garbage delta times.
Those values (256 and 64) are part of the header/descriptor, NOT the actual sample count and size.

**The race time `26335` was NOT found anywhere in this chunk**, which means the start/end times
might be stored differently or we haven't found the right offset for the outer structure.

---

## 5. CGameCtnGhost (82k Zlib Chunk) — Checkpoint Data

The 82k zlib chunk at body offset 1,673,701 contains checkpoint/event position data:

- **5 position triplets** found as raw floats with 40-byte stride starting at offset 69,680:
  ```
  offset=69680: (915.60, 144.00, 1055.01)
  offset=69720: (1074.48, 145.13, 1038.07)
  offset=69760: (1071.50, 144.82, 1038.01)
  offset=69800: (1175.81, 146.00, 1027.91)
  offset=70188: (131.26, 50.36, 543.99)
  ```

These are valid TM2020 Stadium coordinates but represent **checkpoint positions**, not per-frame ghost data.
The 40-byte stride contains: 3 floats (position) + 28 bytes of metadata/flags.

---

## 6. What the Original Parser Gets Wrong

The `tm_gbx` parser (in the repository) fails because:

1. **`find_zlib_ghost_data()`** searches for class ID `0x0A02B000` (`CSceneVehicleCar`) — this ID does NOT exist in TM2020 files
2. **Zlib decompression approach**: The parser likely tries to read `(uncompressed_size, compressed_size)` from bytes preceding zlib headers, but those bytes are NOT size fields in this format
3. **Sample format assumption**: Even if decompression worked, the samples are stored as transposed, delta-accumulated byte columns — NOT as consecutive float records

---

## 7. Next Steps to Fix the Parser

### Step 1: Parse the outer `CPlugEntRecordData` structure correctly
- Need to understand the full `ReadWrite` method from gbx-net (line 151+)
- The chunk `0x0602201A` content needs proper parsing of `EntRecordDescs` before we can find where `ReadEncodedDeltas` is called
- Key source reference: `CPlugEntRecordData.cs` lines 196–292 in [gbx-net](https://github.com/BigBang1112/gbx-net)

### Step 2: Implement `ReadEncodedDeltas` in Python
```python
def read_encoded_deltas(f):
    num_samples = read_i32(f)
    sample_size = read_i32(f)
    
    # Read delta times
    delta_times = [read_i32(f) for _ in range(num_samples)]
    
    # Read column-wise delta-accumulated data
    samples = [bytearray(sample_size) for _ in range(num_samples)]
    for col in range(sample_size):
        col_bytes = f.read(num_samples)
        acc = 0
        for row in range(num_samples):
            acc = (acc + col_bytes[row]) & 0xFF
            samples[row][col] = acc
    
    # Convert delta times to absolute
    abs_times = []
    t = 0
    for dt in delta_times:
        t += dt
        abs_times.append(t)
    
    return abs_times, samples
```

### Step 3: Decode sample bytes into meaningful fields
- Each sample is `sample_size` bytes (likely 64)
- Field layout depends on the `EntRecordDesc` class ID
- For `CSceneVehicleVis` (`0x0A018000`): positions, rotations, wheel data, speed, etc.
- gbx-net's `CSceneVehicleVis.EntRecordDelta` class defines the field layout

### Step 4: Find where `ReadEncodedDeltas` actually starts in chunk `0x0602201A`
- The 165,851 bytes starting at offset 224 need proper header parsing first
- The header contains entity descriptors that define the data format
- We need to parse past: version, entity descriptors, notice descriptors, then reach the entity sample buffers

---

## 8. Code to Reproduce Current State

```python
import lzo, zlib, struct, io
from tm_gbx.header import parse_header
from tm_gbx.reader import read_int32, read_uint32

filepath = "/path/to/Ville (Best).Gbx"

with open(filepath, 'rb') as f:
    header_data = parse_header(f)
    num_external = read_int32(f)
    
    # Decompress LZO body
    uncompressed_size = read_uint32(f)
    compressed_size = read_uint32(f)
    compressed_data = f.read(compressed_size)
    body_data = lzo.decompress(compressed_data, False, uncompressed_size)

# Decompress the 186k zlib chunk (CPlugEntRecordData)
dec = zlib.decompressobj()
record_data = dec.decompress(body_data[1508892:1508892 + 2_000_000])
record_data += dec.flush()
# record_data = 186,943 bytes containing the ghost sample data

# Decompress the 82k zlib chunk (CGameCtnGhost checkpoint data)
dec2 = zlib.decompressobj()
ghost_data = dec2.decompress(body_data[1673701:1673701 + 2_000_000])
ghost_data += dec2.flush()
# ghost_data = 82,500 bytes containing checkpoint positions

# The PIKS chunks inside record_data:
# Chunk 0x0602201A starts at offset 212 (content at 224), size=165,851
chunk_content = record_data[224:224+165851]
# THIS is where the sample data lives, but needs proper header parsing
```

---

## 9. Reference: gbx-net Source Code

The authoritative reference for this format is:
- **Repository**: https://github.com/BigBang1112/gbx-net
- **Key file**: `Src/GBX.NET/Engines/Plug/CPlugEntRecordData.cs`
- **Key methods**:
  - `ReadEntList()` (line 196) — reads the entity sample list
  - `ReadEncodedDeltas()` (line 246) — decodes transposed delta-accumulated samples
  - `ReadEntRecordDeltas()` (line 308) — alternative non-encoded reading
  - `CreateEntRecordDelta()` (line 319) — maps classId to delta type (e.g., `CSceneVehicleVis`)
  - `EntRecordDesc` (line 402) — entity type descriptor

We downloaded this source and confirmed the `ReadEncodedDeltas` algorithm (see Section 7, Step 2).
**Still needed**: Full source of lines 95–200 and 370–494 to understand the outer structure parsing.
