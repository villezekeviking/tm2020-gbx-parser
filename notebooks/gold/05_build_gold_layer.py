"""
Microsoft Fabric Notebook: Build Gold Layer

Reads from Silver Delta tables, builds the track spine dimension,
maps all telemetry to the spine, and writes three Gold tables ready
for Power BI.

Prerequisites:
- silver_replay_header and silver_replay_telemetry Delta tables exist
- Write access to /lakehouse/default/Tables/

Output Gold tables:
- gold_replay_header    : Replay metadata (copy of Silver with ingestion timestamp)
- gold_track_spine      : Track dimension — one row per spatial point per map
- gold_replay_telemetry : All telemetry mapped to spine with enriched columns

Note: The 'spark' variable is pre-defined in Fabric Notebooks.
"""

# ========================================
# Cell 1: Parameters
# ========================================

# Input Silver tables (paths in Lakehouse)
SILVER_HEADER_TABLE    = "/lakehouse/default/Tables/silver_replay_header"
SILVER_TELEMETRY_TABLE = "/lakehouse/default/Tables/silver_replay_telemetry"

# Output Gold tables
GOLD_HEADER_TABLE    = "/lakehouse/default/Tables/gold_replay_header"
GOLD_TRACK_SPINE_TABLE = "/lakehouse/default/Tables/gold_track_spine"
GOLD_TELEMETRY_TABLE = "/lakehouse/default/Tables/gold_replay_telemetry"

# Source filter for track spine — use player replays (not leaderboard)
SPINE_SOURCE = "player"

# ========================================
# Cell 2: Read Silver Tables
# ========================================

from pyspark.sql.functions import (
    col, lit, row_number, min as spark_min, sqrt, pow as spark_pow,
    monotonically_increasing_id, current_timestamp, sum as spark_sum,
    when, coalesce, split, size, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# Read Silver tables
df_header    = spark.read.format("delta").load(SILVER_HEADER_TABLE)
df_telemetry = spark.read.format("delta").load(SILVER_TELEMETRY_TABLE)

print(f"✓ Loaded {df_header.count()} replay headers")
print(f"✓ Loaded {df_telemetry.count()} telemetry rows")

# ========================================
# Cell 3: Gold Replay Header
# ========================================

# Copy Silver header to Gold — Power BI should only connect to Gold
df_gold_header = df_header.withColumn("ingested_to_gold_at", current_timestamp())

# Overwrite mode — safe to rerun without duplicates
df_gold_header.write.format("delta").mode("overwrite").save(GOLD_HEADER_TABLE)

print(f"✓ Wrote {df_gold_header.count()} rows to gold_replay_header")

# ========================================
# Cell 4: Enrich Telemetry with Checkpoint Section
# ========================================

# Add checkpoint_section to each telemetry row.
# Based on the replay's checkpoint crossing times from the header.
#
# NOTE: The checkpoint time format will be confirmed after testing.
# We attempt to parse from the header's 'checkpoints' column, which is
# expected to be a comma-separated string of cumulative ms values
# (e.g. "16000,24000,31000,45000").
# If the column doesn't exist or is null, all rows default to section 1.
#
# Also adds:
#   distance_per_sample  = speed × 0.05  (speed × 50ms in seconds)
#   cumulative_distance  = running total of distance_per_sample per replay

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType as _Int

# Check whether the header has a usable 'checkpoints' column
has_checkpoints = "checkpoints" in df_header.columns

if has_checkpoints:
    # Parse checkpoint times for each replay into an array column
    df_cp = (
        df_header
        .select("replay_id", "checkpoints")
        .withColumn("cp_times", split(col("checkpoints"), ",").cast("array<long>"))
    )
    df_tel_cp = df_telemetry.join(df_cp, on="replay_id", how="left")

    # UDF: find which checkpoint section a given time_ms falls into (1-based)
    def _get_section(time_ms, cp_times):
        if cp_times is None or len(cp_times) == 0:
            return 1
        for i, cp in enumerate(cp_times):
            if time_ms <= cp:
                return i + 1
        return len(cp_times) + 1  # after last checkpoint

    get_section_udf = udf(_get_section, _Int())

    df_enriched = (
        df_tel_cp
        .withColumn("checkpoint_section", get_section_udf(col("time_ms"), col("cp_times")))
        .drop("cp_times", "checkpoints")
    )
    print("✓ Checkpoint sections assigned from header checkpoints column")
else:
    # No checkpoint data yet — default everything to section 1
    # (update once the checkpoints column is populated in Silver)
    df_enriched = df_telemetry.withColumn("checkpoint_section", lit(1).cast(_Int()))
    print("⏭ No 'checkpoints' column found — all rows assigned to checkpoint_section = 1")

# Add distance columns
# distance_per_sample: how far the car travelled in this 50ms window.
# speed is in Trackmania native units per second; × 0.05 converts to per-sample distance.
w_cumulative = Window.partitionBy("replay_id").orderBy("time_ms").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df_enriched_telemetry = (
    df_enriched
    .withColumn("distance_per_sample", col("speed").cast(FloatType()) * lit(0.05))
    .withColumn("cumulative_distance",  spark_sum("distance_per_sample").over(w_cumulative))
)

print("✓ distance_per_sample and cumulative_distance added")

# ========================================
# Cell 5: Build Track Spine
# ========================================

# For each map, find the fastest player run (lowest race_time_ms where source = "player")
# Extract that run's telemetry as the track spine dimension table

# Step 1: Find fastest player replay per map
w_fastest = Window.partitionBy("map_uid").orderBy(col("race_time_ms").asc())

df_fastest = (
    df_header
    .filter(col("source") == SPINE_SOURCE)
    .withColumn("rank", row_number().over(w_fastest))
    .filter(col("rank") == 1)
    .select("replay_id", "map_uid")
)

print("Fastest player runs per map:")
df_fastest.show(truncate=False)

# Step 2: Get those replays' telemetry (already enriched with checkpoint_section)
df_spine_telemetry = (
    df_enriched_telemetry
    .join(df_fastest, on="replay_id", how="inner")
    .select("map_uid", "checkpoint_section", "x", "y", "z", "time_ms")
    .orderBy("map_uid", "time_ms")
)

# Step 3: Add surrogate key (track_point_id) — sequential per map
w_point = Window.partitionBy("map_uid").orderBy("time_ms")
df_track_spine = (
    df_spine_telemetry
    .withColumn("track_point_id", row_number().over(w_point))
    .select("track_point_id", "map_uid", "checkpoint_section", "x", "y", "z")
)

# Overwrite mode — safe to rerun without duplicates
df_track_spine.write.format("delta").mode("overwrite").save(GOLD_TRACK_SPINE_TABLE)

print(f"✓ Wrote {df_track_spine.count()} track spine points")
df_track_spine.show(20, truncate=False)

# ========================================
# Cell 6: Map Telemetry to Track Spine
# ========================================

# For each telemetry row, find the nearest track spine point.
# Only compare within the same map_uid AND checkpoint_section to keep
# the join manageable and the comparisons meaningful.
#
# Distance formula: D = sqrt((x1-x2)² + (y1-y2)² + (z1-z2)²)

# Get map_uid onto telemetry via header join
df_telemetry_with_map = (
    df_enriched_telemetry
    .join(df_header.select("replay_id", "map_uid"), on="replay_id", how="inner")
)

# Rename spine columns to avoid ambiguity during the join
df_spine_renamed = (
    df_track_spine
    .withColumnRenamed("x", "spine_x")
    .withColumnRenamed("y", "spine_y")
    .withColumnRenamed("z", "spine_z")
)

# Join telemetry with spine on same map + same checkpoint section,
# then calculate Euclidean distance to every candidate spine point.
# The spine table is typically small (one run per map), so we broadcast it
# to avoid a full shuffle join.
df_crossed = df_telemetry_with_map.join(
    broadcast(df_spine_renamed),
    on=["map_uid", "checkpoint_section"],
    how="inner"
)

df_with_dist = df_crossed.withColumn(
    "dist",
    sqrt(
        spark_pow(col("x") - col("spine_x"), 2) +
        spark_pow(col("y") - col("spine_y"), 2) +
        spark_pow(col("z") - col("spine_z"), 2)
    )
)

# For each telemetry row, keep only the closest spine point
# (replay_id + time_ms is unique per telemetry row)
w_min_dist = Window.partitionBy("replay_id", "time_ms").orderBy(col("dist").asc())

df_mapped = (
    df_with_dist
    .withColumn("dist_rank", row_number().over(w_min_dist))
    .filter(col("dist_rank") == 1)
    .drop("dist_rank", "spine_x", "spine_y", "spine_z")
    .withColumnRenamed("dist", "distance_to_spine")
)

print(f"✓ Mapped {df_mapped.count()} telemetry rows to track spine")

# ========================================
# Cell 7: Write Gold Telemetry
# ========================================

# Select final columns: all original telemetry fields + new Gold columns
df_gold_telemetry = df_mapped.select(
    "replay_id",
    "map_uid",
    "track_point_id",
    "checkpoint_section",
    "distance_to_spine",
    "distance_per_sample",
    "cumulative_distance",
    # Original telemetry fields
    "time_ms", "time_s",
    "x", "y", "z",
    "speed", "side_speed",
    "vel_x", "vel_y", "vel_z",
    "pitch_deg", "yaw_deg", "roll_deg",
    "steer", "gas", "brake",
    "rpm", "gear",
    "is_turbo", "turbo_time",
    "is_ground_contact", "is_top_contact",
    "fl_dampen", "fr_dampen", "rr_dampen", "rl_dampen",
    "fl_ice", "fr_ice", "rr_ice", "rl_ice",
    "fl_dirt", "fr_dirt", "rr_dirt", "rl_dirt",
    "fl_slip", "fr_slip", "rr_slip", "rl_slip",
    "fl_ground_mat", "fr_ground_mat", "rr_ground_mat", "rl_ground_mat",
    "fl_wheel_rot", "fr_wheel_rot", "rr_wheel_rot", "rl_wheel_rot",
    "reactor_state", "reactor_boost",
    "reactor_pedal", "reactor_steer",
    "wetness", "sim_time_coef",
)

# Overwrite mode — safe to rerun without duplicates
df_gold_telemetry.write.format("delta").mode("overwrite").save(GOLD_TELEMETRY_TABLE)

count = df_gold_telemetry.count()
print(f"✓ Wrote {count} rows to gold_replay_telemetry")

# ========================================
# Cell 8: Summary
# ========================================

print("=" * 60)
print("GOLD LAYER BUILD SUMMARY")
print("=" * 60)

header_count    = spark.read.format("delta").load(GOLD_HEADER_TABLE).count()
spine_count     = spark.read.format("delta").load(GOLD_TRACK_SPINE_TABLE).count()
telemetry_count = spark.read.format("delta").load(GOLD_TELEMETRY_TABLE).count()

print(f"gold_replay_header:     {header_count} rows")
print(f"gold_track_spine:       {spine_count} points")
print(f"gold_replay_telemetry:  {telemetry_count} rows")

print("\n--- Track Spine Sample ---")
spark.read.format("delta").load(GOLD_TRACK_SPINE_TABLE).show(10, truncate=False)

print("\n--- Gold Telemetry Sample ---")
spark.read.format("delta").load(GOLD_TELEMETRY_TABLE).select(
    "replay_id", "track_point_id", "checkpoint_section",
    "distance_to_spine", "speed", "time_ms"
).show(10, truncate=False)

print("=" * 60)
print("Power BI can now connect to Gold tables only")
print("=" * 60)
