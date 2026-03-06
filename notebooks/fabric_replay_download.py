"""
Microsoft Fabric Notebook: Download Trackmania 2020 Leaderboard Replay GBX Files

Connects to the Trackmania Nadeo API, fetches the top N leaderboard records
for each map you specify, and downloads the .Replay.Gbx files into the
Lakehouse Files area.

The downloaded replays land in:
  /lakehouse/default/Files/replays/leaderboard/{map_id}/

From there, you can parse them with the existing tm_gbx parser
and ingest into Delta tables.

Prerequisites:
- A Ubisoft account (email + password) that has played Trackmania 2020
- Write access to /lakehouse/default/Files/

Parameters (edit in Cell 1):
- UBI_EMAIL      : Your Ubisoft account email
- UBI_PASSWORD   : Your Ubisoft account password
- MAP_IDS        : List of map IDs to fetch leaderboard replays for
- TOP_N_PER_MAP  : How many top leaderboard entries to download per map
"""

# ========================================
# Cell 1: Parameters
# ========================================

UBI_EMAIL    = ""   # Your Ubisoft email
UBI_PASSWORD = ""   # Your Ubisoft password

# List of map IDs to fetch leaderboard replays for.
# You can find a map's ID in-game or via the Trackmania exchange website.
MAP_IDS = [
    # "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",  # Example map ID — replace with real ones
]

# How many top leaderboard entries to download per map (e.g. 5 or 10)
TOP_N_PER_MAP = 5

# Output folder in Lakehouse (leaderboard replays go under leaderboard/{map_id}/)
OUTPUT_BASE = "/lakehouse/default/Files/replays"

# ========================================
# Cell 2: Authenticate with Ubisoft + Nadeo
# ========================================

import requests
import base64
import json
import os
import shutil
import time as _time

UBI_AUTH_URL  = "https://public-ubiservices.ubi.com/v3/profiles/sessions"
NADEO_AUTH_URL = "https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices"
UBI_APP_ID    = "86263886-327a-4328-ac69-527f0d20a237"

# Step 1 — Get Ubisoft ticket
basic_token = base64.b64encode(f"{UBI_EMAIL}:{UBI_PASSWORD}".encode()).decode()

ubi_response = requests.post(
    UBI_AUTH_URL,
    headers={
        "Content-Type": "application/json",
        "Ubi-AppId": UBI_APP_ID,
        "Authorization": f"Basic {basic_token}",
        "User-Agent": "tm2020-gbx-parser / fabric-replay-download / contact via GitHub"
    }
)
ubi_response.raise_for_status()
ubi_ticket = ubi_response.json()["ticket"]
print("✓ Ubisoft ticket obtained")

# Step 2 — Exchange for Nadeo token (NadeoServices audience for core API)
nadeo_response = requests.post(
    NADEO_AUTH_URL,
    headers={
        "Content-Type": "application/json",
        "Authorization": f"ubi_v1 t={ubi_ticket}",
        "User-Agent": "tm2020-gbx-parser / fabric-replay-download / contact via GitHub"
    },
    json={"audience": "NadeoServices"}
)
nadeo_response.raise_for_status()
nadeo_token = nadeo_response.json()["accessToken"]
account_id  = nadeo_response.json()["accountId"]

HEADERS = {
    "Authorization": f"nadeo_v1 t={nadeo_token}",
    "User-Agent": "tm2020-gbx-parser / fabric-replay-download / contact via GitHub"
}

print(f"✓ Nadeo token obtained for account: {account_id}")

# ========================================
# Cell 3: Fetch Leaderboard Records
# ========================================

# Base URLs for the two API calls we need per map:
#   1. Leaderboard tops   — returns the top N player positions + account IDs for a map
#   2. Map records lookup — returns the actual records (including replay URL) for those account IDs
LEADERBOARD_URL = "https://prod.trackmania.core.nadeo.online/v2/leaderboard/groups/Personal_Best/maps/{map_id}/tops"
MAP_RECORDS_URL  = "https://prod.trackmania.core.nadeo.online/v2/mapRecords/"

# We'll collect all records to download across all maps here
all_records_to_download = []  # list of dicts: {map_id, account_id, record_id, race_time, replay_url, position}

for map_id in MAP_IDS:
    print(f"\n── Map: {map_id}")

    # --- Step A: Fetch the leaderboard top N for this map ---
    # "onlyWorld=true" means we get the global top, not regional
    leaderboard_response = requests.get(
        LEADERBOARD_URL.format(map_id=map_id),
        params={"length": TOP_N_PER_MAP, "onlyWorld": "true"},
        headers=HEADERS
    )
    leaderboard_response.raise_for_status()
    tops = leaderboard_response.json().get("tops", [])

    if not tops:
        print(f"  ⚠ No leaderboard entries found for map {map_id}")
        continue

    # tops is a list of zone-grouped entries; the first entry is the world top
    world_top = tops[0].get("top", [])
    print(f"  ✓ Leaderboard returned {len(world_top)} entries")

    # Collect the account IDs of the top players
    account_ids = [entry["accountId"] for entry in world_top]
    positions   = {entry["accountId"]: entry["position"] for entry in world_top}

    # --- Step B: Fetch the actual map records for those account IDs ---
    # This gives us the replay download URLs
    records_response = requests.get(
        MAP_RECORDS_URL,
        params={
            "mapIdList":     map_id,
            "accountIdList": ",".join(account_ids),
            "seasonId":      "Personal_Best"
        },
        headers=HEADERS
    )
    records_response.raise_for_status()
    map_records = records_response.json()

    print(f"  ✓ Fetched {len(map_records)} records with replay URLs")

    # Add each record to our download list
    for rec in map_records:
        acc_id = rec.get("accountId", "")
        all_records_to_download.append({
            "map_id":     rec.get("mapId", map_id),
            "account_id": acc_id,
            "record_id":  rec.get("mapRecordId", ""),
            "race_time":  rec.get("recordScore", {}).get("time", 0),
            "replay_url": rec.get("url", ""),
            "position":   positions.get(acc_id, 0),
        })

    # Brief pause between maps to respect rate limits
    _time.sleep(0.6)

print(f"\n✓ Total records to download: {len(all_records_to_download)}")

# Show a preview of what we'll download
for rec in all_records_to_download:
    time_s = rec["race_time"] / 1000
    print(f"  #{rec['position']:>3}  Map: {rec['map_id'][:12]}...  Account: {rec['account_id'][:8]}...  Time: {time_s:.3f}s")

# ========================================
# Cell 4: Download Replay Files
# ========================================

# Leaderboard rankings change over time (new world records get set), so we
# clean out and re-download each map's folder on every run.  This ensures the
# files always reflect the current top N — old entries won't linger.
#
# Output path per map:  replays/leaderboard/{map_id}/
#   File name format:   pos{position:03d}_{race_time_ms}_{record_id}.Replay.Gbx

downloaded = []
skipped    = []
failed     = []

# Group records by map so we can clean one folder at a time
maps_seen = set()

for rec in all_records_to_download:
    map_id     = rec["map_id"]
    record_id  = rec["record_id"]
    race_time  = rec["race_time"]
    position   = rec["position"]
    replay_url = rec["replay_url"]

    # Build output path: replays/leaderboard/{map_id}/
    out_dir = os.path.join(OUTPUT_BASE, "leaderboard", map_id)

    # On first encounter of this map in the current run, wipe the folder so
    # stale entries from a previous run don't accumulate.
    if map_id not in maps_seen:
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
            print(f"🧹 Cleaned stale folder: leaderboard/{map_id}/")
        maps_seen.add(map_id)

    os.makedirs(out_dir, exist_ok=True)

    # File name encodes leaderboard position and race time so it's self-describing
    file_name = f"pos{position:03d}_{race_time}_{record_id}.Replay.Gbx"
    file_path = os.path.join(out_dir, file_name)

    # Skip if already downloaded (can happen if the same record appears twice)
    if os.path.exists(file_path):
        skipped.append(file_name)
        print(f"⏭ Already exists: {file_name}")
        continue

    # Check that there is actually a replay URL to download
    if not replay_url:
        failed.append((file_name, "No replay URL in record"))
        print(f"✗ No replay URL for record {record_id}")
        continue

    # Download the replay file
    try:
        replay_response = requests.get(replay_url, headers=HEADERS)
        replay_response.raise_for_status()

        with open(file_path, "wb") as f:
            f.write(replay_response.content)

        size_kb = len(replay_response.content) / 1024
        downloaded.append(file_name)
        print(f"✓ Downloaded #{position:>3}: {file_name} ({size_kb:.1f} KB)")

    except requests.exceptions.HTTPError as e:
        failed.append((file_name, str(e)))
        print(f"✗ Failed #{position}: {file_name} — {e}")

    # Respect the Nadeo rate limit (~2 requests/second)
    _time.sleep(0.6)

# ========================================
# Cell 5: Summary
# ========================================

print("=" * 60)
print("DOWNLOAD SUMMARY")
print("=" * 60)
print(f"Maps processed  : {len(MAP_IDS)}")
print(f"Top N per map   : {TOP_N_PER_MAP}")
print(f"Records found   : {len(all_records_to_download)}")
print(f"Downloaded      : {len(downloaded)}")
print(f"Already existed : {len(skipped)}")
print(f"Failed          : {len(failed)}")
print(f"Output folder   : {OUTPUT_BASE}/leaderboard/")
print("=" * 60)

if failed:
    print("\nFailed downloads:")
    for name, err in failed:
        print(f"  ✗ {name}: {err}")

# List all downloaded files
print("\nFiles in Lakehouse:")
leaderboard_dir = os.path.join(OUTPUT_BASE, "leaderboard")
if os.path.exists(leaderboard_dir):
    for root, dirs, files in os.walk(leaderboard_dir):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), OUTPUT_BASE)
            print(f"  📄 {rel}")
