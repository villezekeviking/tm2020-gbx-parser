"""
Microsoft Fabric Notebook: Download Trackmania 2020 Leaderboard Replay GBX Files

Connects to the Trackmania Nadeo API, fetches the top N leaderboard records
for each map you specify, and downloads the .Replay.Gbx files into the
Lakehouse Files area.

In addition to the top N leaderboard entries, you can supply a list of
tracked player Account IDs.  For each map the script will also look up
those players' personal bests and download their replays (even if they
are not in the top N).

The downloaded replays land in:
  /lakehouse/default/Files/replays/leaderboard/{map_uid}/

From there, you can parse them with the existing tm_gbx parser
and ingest into Delta tables.

Prerequisites:
- A Ubisoft account (email + password) that has played Trackmania 2020
- Write access to /lakehouse/default/Files/

Parameters (edit in Cell 1):
- UBI_EMAIL             : Your Ubisoft account email
- UBI_PASSWORD          : Your Ubisoft account password
- MAP_UIDS              : List of map UIDs to fetch leaderboard replays for
- TOP_N_PER_MAP         : How many top leaderboard entries to download per map
- TRACKED_ACCOUNT_IDS   : Account IDs of players to always include (if they
                          have a record on the map)
"""

# ========================================
# Cell 1: Parameters
# ========================================

UBI_EMAIL    = ""   # Your Ubisoft email
UBI_PASSWORD = ""   # Your Ubisoft password

# List of map UIDs to fetch leaderboard replays for.
# A map UID is the short ~27-character string you see in Trackmania URLs
# or in-game (e.g. "KRelvYHRjEQoqnkJ_Th8FLKzTsg").  It is NOT the long UUID.
# On trackmania.io the UID is the SECOND part of the URL after the UUID.
MAP_UIDS = [
    # "KRelvYHRjEQoqnkJ_Th8FLKzTsg",  # Example map UID — replace with real ones
]

# How many top leaderboard entries to download per map (e.g. 5 or 10)
TOP_N_PER_MAP = 5

# Account IDs of players to always include (if they have a record on the map).
# These are UUID-format strings like "36a476de-c712-45c4-b769-247b5dcb3f03".
# Find account IDs on trackmania.io player profiles.
TRACKED_ACCOUNT_IDS = [
    # "36a476de-c712-45c4-b769-247b5dcb3f03",  # Example — replace with real ones
]

# Output folder in Lakehouse (leaderboard replays go under leaderboard/{map_uid}/)
OUTPUT_BASE = "/lakehouse/default/Files/replays"

# ========================================
# Cell 2: Authenticate with Ubisoft + Nadeo
# ========================================

import requests
import base64
import os
import shutil
import time as _time

UBI_AUTH_URL   = "https://public-ubiservices.ubi.com/v3/profiles/sessions"
NADEO_AUTH_URL = "https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices"
UBI_APP_ID     = "86263886-327a-4328-ac69-527f0d20a237"

USER_AGENT = "tm2020-gbx-parser / fabric-replay-download / contact via GitHub"

# Step 1 — Get Ubisoft ticket
basic_token = base64.b64encode(f"{UBI_EMAIL}:{UBI_PASSWORD}".encode()).decode()

ubi_response = requests.post(
    UBI_AUTH_URL,
    headers={
        "Content-Type": "application/json",
        "Ubi-AppId": UBI_APP_ID,
        "Authorization": f"Basic {basic_token}",
        "User-Agent": USER_AGENT,
    }
)
ubi_response.raise_for_status()
ubi_ticket = ubi_response.json()["ticket"]
print("✓ Ubisoft ticket obtained")

# Step 2 — Exchange for TWO Nadeo tokens:
#   - NadeoLiveServices token → used to query the leaderboard (live-services host)
#   - NadeoServices token     → used to look up map info and download replay URLs (core host)
#
# Note: the Nadeo token response only contains 'accessToken' and 'refreshToken'.
# There is no 'accountId' field in the response.

def get_nadeo_token(ubi_ticket, audience):
    """Exchange a Ubisoft ticket for a Nadeo access token for the given audience."""
    resp = requests.post(
        NADEO_AUTH_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"ubi_v1 t={ubi_ticket}",
            "User-Agent": USER_AGENT,
        },
        json={"audience": audience}
    )
    resp.raise_for_status()
    return resp.json()["accessToken"]

live_token = get_nadeo_token(ubi_ticket, "NadeoLiveServices")
core_token = get_nadeo_token(ubi_ticket, "NadeoServices")

# Build reusable headers for each service
LIVE_HEADERS = {
    "Authorization": f"nadeo_v1 t={live_token}",
    "User-Agent": USER_AGENT,
}
CORE_HEADERS = {
    "Authorization": f"nadeo_v1 t={core_token}",
    "User-Agent": USER_AGENT,
}

print("✓ NadeoLiveServices token obtained")
print("✓ NadeoServices token obtained")

# ========================================
# Cell 3: Fetch Leaderboard Records
# ========================================

# For each map UID we need three API calls:
#
#   A. Map info lookup (core API)
#      GET https://prod.trackmania.core.nadeo.online/maps/?mapUidList={map_uid}
#      → returns the internal map UUID (mapId) we need for records lookup
#
#   B. Leaderboard top N (live API)
#      GET https://live-services.trackmania.nadeo.live/api/token/leaderboard/group/Personal_Best/map/{map_uid}/top
#      → returns top N entries with accountId, position, and score (race time in ms)
#
#   C. Map records with replay URLs (core API)
#      GET https://prod.trackmania.core.nadeo.online/mapRecords/
#      IMPORTANT: use /mapRecords/ (no /v2/ prefix!) — /v2/mapRecords/ returns 400.
#      → returns records including the 'url' field for downloading the replay

MAP_INFO_URL    = "https://prod.trackmania.core.nadeo.online/maps/"
LEADERBOARD_URL = "https://live-services.trackmania.nadeo.live/api/token/leaderboard/group/Personal_Best/map/{map_uid}/top"
SURROUND_URL    = "https://live-services.trackmania.nadeo.live/api/token/leaderboard/group/Personal_Best/map/{map_uid}/surround/0/0"
MAP_RECORDS_URL = "https://prod.trackmania.core.nadeo.online/mapRecords/"

# We'll collect all records to download across all maps here
all_records_to_download = []

# Counters for the summary
tracked_found   = 0
tracked_looked_up = 0

for map_uid in MAP_UIDS:
    print(f"\n── Map UID: {map_uid}")

    # --- Step A: Look up the internal map UUID from the map UID ---
    map_info_response = requests.get(
        MAP_INFO_URL,
        params={"mapUidList": map_uid},
        headers=CORE_HEADERS,
    )
    map_info_response.raise_for_status()
    map_info_list = map_info_response.json()

    if not map_info_list:
        print(f"  ⚠ Map info not found for UID {map_uid} — skipping")
        continue

    map_uuid = map_info_list[0]["mapId"]
    map_name = map_info_list[0].get("name", "?")
    print(f"  ✓ {map_name}  (UUID: {map_uuid})")

    _time.sleep(0.6)

    # --- Step B: Fetch the leaderboard top N for this map ---
    leaderboard_response = requests.get(
        LEADERBOARD_URL.format(map_uid=map_uid),
        params={"onlyWorld": "true", "length": TOP_N_PER_MAP},
        headers=LIVE_HEADERS,
    )
    leaderboard_response.raise_for_status()
    tops = leaderboard_response.json().get("tops", [])

    if not tops:
        print(f"  ⚠ No leaderboard entries found for map {map_uid}")
        continue

    world_top = tops[0].get("top", [])
    print(f"  ✓ Leaderboard returned {len(world_top)} entries")

    account_ids = [entry["accountId"] for entry in world_top]
    positions   = {entry["accountId"]: entry["position"] for entry in world_top}
    scores      = {entry["accountId"]: entry["score"] for entry in world_top}

    _time.sleep(0.6)

    # --- Step C: Fetch the actual map records for those account IDs ---
    records_response = requests.get(
        MAP_RECORDS_URL,
        params={
            "mapIdList":     map_uuid,
            "accountIdList": ",".join(account_ids),
        },
        headers=CORE_HEADERS,
    )
    records_response.raise_for_status()
    map_records = records_response.json()

    print(f"  ✓ Fetched {len(map_records)} records with replay URLs")

    for rec in map_records:
        acc_id = rec.get("accountId", "")
        all_records_to_download.append({
            "map_uid":    map_uid,
            "map_uuid":   map_uuid,
            "account_id": acc_id,
            "record_id":  rec.get("mapRecordId", ""),
            "race_time":  scores.get(acc_id, 0),
            "replay_url": rec.get("url", ""),
            "position":   positions.get(acc_id, 0),
        })

    _time.sleep(0.6)

    # --- Step D: Fetch records for tracked players not already in the top N ---
    top_n_account_ids = set(account_ids)
    players_to_look_up = [a for a in TRACKED_ACCOUNT_IDS if a not in top_n_account_ids]

    for tracked_id in players_to_look_up:
        tracked_looked_up += 1

        try:
            surround_response = requests.get(
                SURROUND_URL.format(map_uid=map_uid),
                params={"onlyWorld": "true", "accountIdList": tracked_id},
                headers=LIVE_HEADERS,
            )
            surround_response.raise_for_status()
            surround_tops = surround_response.json().get("tops", [])
        except Exception as e:
            print(f"  ✗ Surround lookup failed for {tracked_id}: {e}")
            _time.sleep(0.6)
            continue

        surround_entries = surround_tops[0].get("top", []) if surround_tops else []

        # Filter to just the tracked player (the surround may include neighbours)
        player_entries = [e for e in surround_entries if e.get("accountId") == tracked_id]

        if not player_entries:
            print(f"  ⏭ Tracked player {tracked_id[:8]}... has no record on this map — skipping")
            _time.sleep(0.6)
            continue

        player_entry = player_entries[0]
        tracked_position = player_entry["position"]
        tracked_score    = player_entry["score"]

        _time.sleep(0.6)

        # Fetch the replay URL for this player via the mapRecords endpoint
        try:
            tracked_rec_response = requests.get(
                MAP_RECORDS_URL,
                params={
                    "mapIdList":     map_uuid,
                    "accountIdList": tracked_id,
                },
                headers=CORE_HEADERS,
            )
            tracked_rec_response.raise_for_status()
            tracked_records = tracked_rec_response.json()
        except Exception as e:
            print(f"  ✗ Map record lookup failed for tracked player {tracked_id[:8]}...: {e}")
            _time.sleep(0.6)
            continue

        if not tracked_records:
            print(f"  ⏭ No map record returned for tracked player {tracked_id[:8]}... — skipping")
            _time.sleep(0.6)
            continue

        trec = tracked_records[0]
        all_records_to_download.append({
            "map_uid":    map_uid,
            "map_uuid":   map_uuid,
            "account_id": tracked_id,
            "record_id":  trec.get("mapRecordId", ""),
            "race_time":  tracked_score,
            "replay_url": trec.get("url", ""),
            "position":   tracked_position,
        })
        tracked_found += 1
        print(f"  ✓ Tracked player {tracked_id[:8]}... found at position #{tracked_position} ({tracked_score / 1000:.3f}s)")

        _time.sleep(0.6)

print(f"\n✓ Total records to download: {len(all_records_to_download)}")

for rec in all_records_to_download:
    time_s = rec["race_time"] / 1000
    pos = rec["position"]
    uid = rec["map_uid"][:12]
    acc = rec["account_id"][:8]
    print(f"  #${pos:>3}  Map: {uid}...  Account: {acc}...  Time: {time_s:.3f}s")

# ========================================
# Cell 4: Download Replay Files
# ========================================

# Leaderboard rankings change over time (new world records), so we clean out
# and re-download each map's folder on every run to keep it current.

downloaded = []
skipped    = []
failed     = []
maps_seen  = set()

for rec in all_records_to_download:
    map_uid    = rec["map_uid"]
    record_id  = rec["record_id"]
    race_time  = rec["race_time"]
    position   = rec["position"]
    account_id = rec["account_id"]
    replay_url = rec["replay_url"]

    out_dir = os.path.join(OUTPUT_BASE, "leaderboard", map_uid)

    # First time we see this map: wipe old files
    if map_uid not in maps_seen:
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
            print(f"🧹 Cleaned stale folder: leaderboard/{map_uid}/")
        maps_seen.add(map_uid)

    os.makedirs(out_dir, exist_ok=True)

    file_name = f"pos{position:03d}_{race_time}_{account_id}_{record_id}.Replay.Gbx"
    file_path = os.path.join(out_dir, file_name)

    if os.path.exists(file_path):
        skipped.append(file_name)
        print(f"⏭ Already exists: {file_name}")
        continue

    if not replay_url:
        failed.append((file_name, "No replay URL in record"))
        print(f"✗ No replay URL for record {record_id}")
        continue

    try:
        replay_response = requests.get(replay_url)
        replay_response.raise_for_status()

        with open(file_path, "wb") as f:
            f.write(replay_response.content)

        size_kb = len(replay_response.content) / 1024
        downloaded.append(file_name)
        print(f"✓ Downloaded #${position:>3}: {file_name} ({size_kb:.1f} KB)")

    except requests.exceptions.HTTPError as e:
        failed.append((file_name, str(e)))
        print(f"✗ Failed #${position}: {file_name} — {e}")

    _time.sleep(0.6)

# ========================================
# Cell 5: Summary
# ========================================

print("=" * 60)
print("DOWNLOAD SUMMARY")
print("=" * 60)
print(f"Maps processed       : {len(MAP_UIDS)}")
print(f"Top N per map        : {TOP_N_PER_MAP}")
print(f"Records found        : {len(all_records_to_download)}")
print(f"Tracked players      : {len(TRACKED_ACCOUNT_IDS)} configured, {tracked_looked_up} looked up, {tracked_found} found")
print(f"Downloaded           : {len(downloaded)}")
print(f"Already existed      : {len(skipped)}")
print(f"Failed               : {len(failed)}")
print(f"Output folder        : {OUTPUT_BASE}/leaderboard/")
print("=" * 60)

if failed:
    print("\nFailed downloads:")
    for name, err in failed:
        print(f"  ✗ {name}: {err}")

print("\nFiles in Lakehouse:")
leaderboard_dir = os.path.join(OUTPUT_BASE, "leaderboard")
if os.path.exists(leaderboard_dir):
    for root, dirs, files in os.walk(leaderboard_dir):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), OUTPUT_BASE)
            print(f"  📄 {rel}")