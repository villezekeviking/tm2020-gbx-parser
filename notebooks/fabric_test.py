"""
Microsoft Fabric Notebook Example for TM2020 GBX Parser

This script demonstrates how to use the tm_gbx parser in a Fabric Notebook
to parse TrackMania 2020 replay files and create Spark DataFrames.

Note: The 'spark' session is automatically available in Fabric Notebooks.
"""

# ========================================
# Cell 1: Import and setup
# ========================================

from tm_gbx import parse_gbx
import os

# Parse a single test file
test_file = "tests/Ville (Best).Gbx"

if os.path.exists(test_file):
    result = parse_gbx(test_file)
    
    print("=" * 60)
    print("METADATA")
    print("=" * 60)
    for key, value in result['metadata'].items():
        print(f"{key}: {value}")
    
    print("\n" + "=" * 60)
    print("GHOST INFO")
    print("=" * 60)
    if result['ghost_info']:
        for key, value in result['ghost_info'].items():
            print(f"{key}: {value}")
    else:
        print("No ghost info available (requires python-lzo)")
    
    print("\n" + "=" * 60)
    print("FIRST 10 GHOST SAMPLES")
    print("=" * 60)
    ghost_samples = result['ghost_samples']
    for i, sample in enumerate(ghost_samples[:10]):
        print(f"Sample {i}: time={sample['time_ms']}ms, "
              f"pos=({sample['position']['x']:.2f}, "
              f"{sample['position']['y']:.2f}, "
              f"{sample['position']['z']:.2f})")
    
    print(f"\nTotal samples: {len(ghost_samples)}")
else:
    print(f"Test file not found: {test_file}")


# ========================================
# Cell 2: Create Spark DataFrame from ghost samples
# ========================================

# Note: 'spark' is automatically available in Fabric Notebooks
# No need to create SparkSession manually

if os.path.exists(test_file):
    result = parse_gbx(test_file)
    ghost_samples = result['ghost_samples']
    
    if len(ghost_samples) > 0:
        # Flatten the structure for Spark DataFrame
        flattened = []
        for sample in ghost_samples:
            flattened.append({
                'time_ms': sample['time_ms'],
                'pos_x': sample['position']['x'],
                'pos_y': sample['position']['y'],
                'pos_z': sample['position']['z']
            })
        
        # Create DataFrame
        df = spark.createDataFrame(flattened)
        
        print("Ghost Samples DataFrame:")
        df.show(10)
        
        print(f"\nDataFrame schema:")
        df.printSchema()
    else:
        print("No ghost samples available (requires python-lzo for decompression)")


# ========================================
# Cell 3: Batch parse all test files
# ========================================

test_files = [
    "tests/Johan (Best).Gbx",
    "tests/Johan (First).Gbx",
    "tests/Jon (Best).Gbx",
    "tests/Jon (First).Gbx",
    "tests/Oskar (Best).gbx",
    "tests/Oskar (First).gbx",
    "tests/Ville (Best).Gbx",
    "tests/Ville (First).Gbx"
]

all_metadata = []

for filepath in test_files:
    if os.path.exists(filepath):
        try:
            result = parse_gbx(filepath)
            metadata = result['metadata']
            metadata['file_name'] = os.path.basename(filepath)
            all_metadata.append(metadata)
            print(f"✓ Parsed {os.path.basename(filepath)}")
        except Exception as e:
            print(f"✗ Failed to parse {os.path.basename(filepath)}: {e}")
    else:
        print(f"✗ File not found: {filepath}")

print(f"\n\nSuccessfully parsed {len(all_metadata)} files")

if len(all_metadata) > 0:
    # Create DataFrame from metadata
    df_metadata = spark.createDataFrame(all_metadata)
    
    print("\nMetadata DataFrame:")
    df_metadata.show(truncate=False)


# ========================================
# Cell 4: Summary statistics
# ========================================

if len(all_metadata) > 0:
    df_metadata.createOrReplaceTempView("replay_metadata")
    
    # Query with SQL
    spark.sql("""
        SELECT 
            player_nickname,
            COUNT(*) as num_replays,
            MIN(race_time_ms) as best_time_ms,
            AVG(race_time_ms) as avg_time_ms
        FROM replay_metadata
        WHERE race_time_ms IS NOT NULL
        GROUP BY player_nickname
        ORDER BY best_time_ms
    """).show()
