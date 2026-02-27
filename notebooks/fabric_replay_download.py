"""
Microsoft Fabric Notebook: Download Trackmania 2020 Replay GBX Files

Connects to the Trackmania Nadeo API, fetches map records for your account,
and downloads the .Replay.Gbx files into the Lakehouse Files area.

The downloaded replays land in:
  /lakehouse/default/Files/replays/{year}/{month}/{map_id}/

From there, you can parse them with the existing tm_gbx parser
and ingest into Delta tables.

Prerequisites:
- A Ubisoft account (email + password) that has played Trackmania 2020
- Write access to /lakehouse/default/Files/

Parameters (edit in Cell 1):
- UBI_EMAIL      : Your Ubisoft account email
- UBI_PASSWORD   : Your Ubisoft account password
- MODE           : "recent" (top N) or "days" (last N days)
- TOP_N          : Number of most recent records to fetch (when MODE = "recent")
- DAYS_AGO       : Number of days to look back (when MODE = "days")
"""

# ========================================
# Cell 1: Parameters
# ========================================

UBI_EMAIL    = ""   # Your Ubisoft email
UBI_PASSWORD = ""   # Your Ubisoft password

# Filter mode: "recent" = top N most recent, "days" = records from last N days
MODE     = "recent"
TOP_N    = 10
DAYS_AGO = 7

# Output folder in Lakehouse
OUTPUT_BASE = "/lakehouse/default/Files/replays"

# ========================================
# Cell 2: Authenticate with Ubisoft + Nadeo
# ========================================

import requests
import base64
import json
import os
import time as _time
from datetime import datetime, timedelta, timezone

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
# Cell 3: Fetch Map Records
# ========================================

RECORDS_URL = f"https://prod.trackmania.core.nadeo.online/v2/accounts/{account_id}/mapRecords"

records_response = requests.get(RECORDS_URL, headers=HEADERS)
records_response.raise_for_status()
all_records = records_response.json()

print(f"✓ Fetched {len(all_records)} total map records from API")

# Filter records based on MODE
if MODE == "days":
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_AGO)
    records = [
        r for r in all_records
        if datetime.fromisoformat(r["timestamp"].replace("+00:00", "+00:00")) >= cutoff
    ]
    records.sort(key=lambda r: r["timestamp"], reverse=True)
    print(f"✓ Filtered to {len(records)} records from last {DAYS_AGO} days")

elif MODE == "recent":
    records = sorted(all_records, key=lambda r: r["timestamp"], reverse=True)[:TOP_N]
    print(f"✓ Selected top {len(records)} most recent records")

else:
    raise ValueError(f"Unknown MODE: {MODE}. Use 'recent' or 'days'.")

# Show what we got
for r in records:
    time_s = r["recordScore"]["time"] / 1000
    print(f"  Map: {r['mapId'][:12]}...  Time: {time_s:.3f}s  Medal: {r['medal']}  Date: {r['timestamp'][:10]}")

# ========================================
# Cell 4: Download Replay Files
# ========================================

downloaded = []
skipped    = []
failed     = []

for r in records:
    map_id    = r["mapId"]
    record_id = r["mapRecordId"]
    timestamp = r["timestamp"][:10]  # YYYY-MM-DD
    race_time = r["recordScore"]["time"]

    # Parse date for folder structure
    dt = datetime.fromisoformat(r["timestamp"].replace("+00:00", "+00:00"))
    year  = str(dt.year)
    month = f"{dt.month:02d}"

    # Build output path: replays/{year}/{month}/{map_id}/
    out_dir = os.path.join(OUTPUT_BASE, year, month, map_id)
    os.makedirs(out_dir, exist_ok=True)

    # File name: {record_id}_{race_time_ms}.Replay.Gbx
    file_name = f"{record_id}_{race_time}.Replay.Gbx"
    file_path = os.path.join(out_dir, file_name)

    # Skip if already downloaded
    if os.path.exists(file_path):
        skipped.append(file_name)
        print(f"⏭ Already exists: {file_name}")
        continue

    # Download the replay
    replay_url = r.get("url", "")
    if not replay_url:
        failed.append((file_name, "No replay URL in record"))
        print(f"✗ No replay URL for record {record_id}")
        continue

    try:
        replay_response = requests.get(replay_url, headers=HEADERS)
        replay_response.raise_for_status()

        with open(file_path, "wb") as f:
            f.write(replay_response.content)

        size_kb = len(replay_response.content) / 1024
        downloaded.append(file_name)
        print(f"✓ Downloaded: {file_name} ({size_kb:.1f} KB)")

    except requests.exceptions.HTTPError as e:
        failed.append((file_name, str(e)))
        print(f"✗ Failed: {file_name} — {e}")

    # Respect rate limit (~2 req/s)
    _time.sleep(0.6)

# ========================================
# Cell 5: Summary
# ========================================

print("=" * 60)
print("DOWNLOAD SUMMARY")
print("=" * 60)
print(f"Mode            : {MODE} ({'top ' + str(TOP_N) if MODE == 'recent' else 'last ' + str(DAYS_AGO) + ' days'})")
print(f"Records found   : {len(records)}")
print(f"Downloaded      : {len(downloaded)}")
print(f"Already existed : {len(skipped)}")
print(f"Failed          : {len(failed)}")
print(f"Output folder   : {OUTPUT_BASE}")
print("=" * 60)

if failed:
    print("\nFailed downloads:")
    for name, err in failed:
        print(f"  ✗ {name}: {err}")

# List all downloaded files
print("\nFiles in Lakehouse:")
for root, dirs, files in os.walk(OUTPUT_BASE):
    for f in files:
        rel = os.path.relpath(os.path.join(root, f), OUTPUT_BASE)
        print(f"  📄 {rel}")
