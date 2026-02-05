# Troubleshooting Guide

## Ghost Sample Extraction Issues

### Investigation Summary (2026-02-05)

We conducted extensive investigation into extracting ghost sample data from TM2020 GBX replay files. Here are the findings:

#### Test File: `tests/Ville (Best).Gbx`

**File Properties:**
- Size: 1,671,079 bytes
- GBX Version: 6
- Class ID: 0x03093000 (CGameCtnReplayRecord)
- Race time: 26,335ms
- Checkpoints: 7 (times: 6148ms, 9505ms, 12487ms, 15825ms, 19408ms, 22768ms, 26335ms)

**Chunk Analysis:**

| Chunk ID | Position | Size | Status | Description |
|----------|----------|------|--------|-------------|
| 0x03093000 | Header | 127 bytes | ✓ Parsed | Map info, time, player nickname/login |
| 0x03093014 | 1,637,826 | 10 bytes | ✓ Found | Ghost data array (new format) |
| 0x03092000 | 1,637,839 | Variable | ✓ Found | CGameCtnGhost class node |
| 0x0309200A | 1,669,865 | **4 bytes** | ⚠️ **EMPTY** | Ghost samples chunk (only zeros) |
| 0x0309200B | 1,669,881 | 60 bytes | ✓ Parsed | Checkpoint times |

**Key Finding:** The ghost samples chunk (0x0309200A) contains only 4 bytes of zeros, indicating **no telemetry data is stored**.

#### What We Tried

1. ✓ Direct chunk parsing at known positions
2. ✓ Scanned entire file for LZO compressed blocks
3. ✓ Searched for all chunk IDs related to ghosts
4. ✓ Tried multiple decompression methods (LZO, zlib)
5. ✓ Attempted various record size structures (20, 22, 24, 28 bytes)
6. ✓ Validated chunk structure against C# GBX.NET implementation
7. ✗ **No ghost sample data found**

#### Possible Explanations

1. **Header-only replay**: File was saved without full ghost data
2. **Game mode specific**: Some TM2020 modes don't save telemetry
3. **Version incompatibility**: Different game build uses different format
4. **External processing**: File was stripped/converted by external tool
5. **Alternative storage**: Ghost data stored in different chunks (not yet identified)

### What DOES Work

The current parser successfully extracts:
- ✓ Player nickname and login
- ✓ Map UID and name
- ✓ Race time
- ✓ Checkpoint times
- ✓ Title ID

### Next Steps

To add ghost sample extraction support:

1. **Test with different replay files**: Try replays from:
   - Different game modes (Campaign, Online, Local)
   - Different TM2020 versions
   - Different save methods (auto-save vs manual save)

2. **Identify valid sample files**: Find replays that DO contain ghost data

3. **Compare file structures**: Diff between files with/without ghost data

4. **Check other chunks**: Investigate chunks we haven't parsed yet:
   - 0x03093024 (CPlugEntRecordData)
   - 0x03093026 (EntDataSceneUIdsToGhost)
   - Other unknown chunks

### How to Verify Ghost Data Presence

Run this diagnostic script:

```python
from tm_gbx import GBXParser
import struct

def diagnose_gbx_file(filepath):
    with open(filepath, 'rb') as f:
        data = f.read()
    
    # Search for samples chunk
    chunk_id = struct.pack('<I', 0x0309200A)
    
    print(f"File: {filepath}")
    print(f"Size: {len(data):,} bytes")
    print("\nSearching for chunk 0x0309200A (Ghost Samples)...")
    
    pos = data.find(chunk_id)
    if pos != -1:
        print(f"  Found at position: {pos:,}")
        
        # Read chunk size
        chunk_size = struct.unpack('<I', data[pos+4:pos+8])[0]
        print(f"  Chunk size: {chunk_size} bytes")
        
        if chunk_size == 0:
            print("  ⚠️  Chunk is EMPTY (size = 0)")
        else:
            # Show first few bytes
            chunk_data = data[pos+8:pos+8+min(chunk_size, 32)]
            print(f"  First bytes: {chunk_data.hex()}")
            
            if all(b == 0 for b in chunk_data):
                print("  ⚠️  Chunk contains only ZEROS")
            else:
                print("  ✓ Chunk contains data")
    else:
        print("  ✗ Chunk NOT FOUND")
    
    # Search for LZO compressed blocks
    print("\nSearching for LZO compressed blocks...")
    lzo_found = 0
    search_pos = 0
    while True:
        # LZO blocks often start with uncompressed size followed by compressed size
        # Look for reasonable size values (> 100, < 1MB)
        pos = data.find(b'LZO', search_pos)
        if pos == -1:
            break
        lzo_found += 1
        print(f"  Possible LZO marker at position: {pos:,}")
        search_pos = pos + 1
    
    if lzo_found == 0:
        print("  ✗ No LZO markers found")
    
    print("\n" + "="*60)

# Example usage:
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        diagnose_gbx_file(sys.argv[1])
    else:
        diagnose_gbx_file("tests/Ville (Best).Gbx")
```

### References

- **GBX.NET**: https://github.com/BigBang1112/gbx-net
  - C# implementation with extensive chunk documentation
  - See `Engines/Game/CGameCtnGhost.cs` for ghost chunk structures

- **TM Format Documentation**: https://wiki.xaseco.org/wiki/GBX
  - Legacy format documentation (TM Nations/Forever era)
  - Some chunks still compatible with TM2020

### Contributing

If you find a TM2020 replay file that DOES contain ghost sample data:

1. Run the diagnostic script above
2. Share the file properties and chunk analysis
3. Open an issue with:
   - File source (game mode, version, how it was saved)
   - Diagnostic script output
   - The replay file (if possible)

This will help us understand the correct format and complete the ghost extraction feature.
