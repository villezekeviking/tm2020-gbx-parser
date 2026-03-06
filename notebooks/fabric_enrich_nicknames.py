"""
Microsoft Fabric Notebook: Enrich silver_replay_header with Player Nicknames

Reads rows from silver_replay_header where account_id is set but player_nickname
is empty, looks up the display names via the Nadeo API in batches, and updates
the table using a MERGE statement.

Prerequisites:
- A Ubisoft account (email + password) that has played Trackmania 2020
- silver_replay_header Delta table must already exist (run fabric_ghost_ingest.py first)

Parameters (edit in Cell 1):
- UBI_EMAIL    : Your Ubisoft account email
- UBI_PASSWORD : Your Ubisoft account password
"""

# ========================================
# Cell 1: Parameters
# ========================================

UBI_EMAIL    = ""   # Your Ubisoft email
UBI_PASSWORD = ""   # Your Ubisoft password

# How many account IDs to look up per API call (max 50 per Nadeo API docs)
BATCH_SIZE = 50

# ========================================
# Cell 2: Authenticate with Ubisoft + Nadeo
# ========================================

import requests
import base64

UBI_AUTH_URL   = "https://public-ubiservices.ubi.com/v3/profiles/sessions"
NADEO_AUTH_URL = "https://prod.trackmania.core.nadeo.online/v2/authentication/token/ubiservices"
UBI_APP_ID     = "86263886-327a-4328-ac69-527f0d20a237"

USER_AGENT = "tm2020-gbx-parser / fabric-enrich-nicknames / contact via GitHub"

# Step 1 — Get Ubisoft ticket
basic_token = base64.b64encode(f"{UBI_EMAIL}:{UBI_PASSWORD}".encode()).decode()

try:
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
except Exception as e:
    raise RuntimeError(f"Ubisoft authentication failed — check UBI_EMAIL / UBI_PASSWORD. Details: {e}")
ubi_ticket = ubi_response.json()["ticket"]
print("✓ Ubisoft ticket obtained")

# Step 2 — Exchange for a NadeoServices token (used for display name lookup)
try:
    resp = requests.post(
        NADEO_AUTH_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"ubi_v1 t={ubi_ticket}",
            "User-Agent": USER_AGENT,
        },
        json={"audience": "NadeoServices"}
    )
    resp.raise_for_status()
except Exception as e:
    raise RuntimeError(f"Nadeo token exchange failed. Details: {e}")
core_token = resp.json()["accessToken"]

CORE_HEADERS = {
    "Authorization": f"nadeo_v1 t={core_token}",
    "User-Agent": USER_AGENT,
}
print("✓ Nadeo token obtained")

# ========================================
# Cell 3: Find rows that need a nickname
# ========================================

df = spark.table("silver_replay_header")

missing = (
    df.filter((df.account_id != "") & (df.player_nickname == ""))
      .select("account_id")
      .distinct()
      .rdd.flatMap(lambda r: [r.account_id])
      .collect()
)

print(f"Found {len(missing)} account(s) with no nickname")

# ========================================
# Cell 4: Look up display names in batches
# ========================================

DISPLAY_NAMES_URL = "https://prod.trackmania.core.nadeo.online/accounts/displayNames/"

nicknames = {}  # account_id -> display_name

for i in range(0, len(missing), BATCH_SIZE):
    batch = missing[i:i + BATCH_SIZE]
    try:
        response = requests.get(
            DISPLAY_NAMES_URL,
            headers=CORE_HEADERS,
            params={"accountIdList": ",".join(batch)},
        )
        response.raise_for_status()
        for entry in response.json():
            nicknames[entry["accountId"]] = entry["displayName"]
        print(f"  Batch {i // BATCH_SIZE + 1}: looked up {len(batch)} account(s)")
    except Exception as e:
        print(f"  ✗ Batch {i // BATCH_SIZE + 1} failed: {e} — skipping")

print(f"\n✓ Resolved {len(nicknames)} nickname(s)")

# ========================================
# Cell 5: Update silver_replay_header
# ========================================

if nicknames:
    # Build a small DataFrame of (account_id, player_nickname) to MERGE from
    updates = spark.createDataFrame(
        [{"account_id": k, "player_nickname": v} for k, v in nicknames.items()]
    )
    updates.createOrReplaceTempView("nickname_updates")

    spark.sql("""
        MERGE INTO silver_replay_header AS target
        USING nickname_updates AS src
        ON target.account_id = src.account_id
        WHEN MATCHED AND target.player_nickname = ''
            THEN UPDATE SET target.player_nickname = src.player_nickname
    """)

    print(f"✓ Updated {len(nicknames)} row(s) in silver_replay_header")
else:
    print("Nothing to update")

# ========================================
# Cell 6: Summary
# ========================================

print("\nUpdated rows:")
spark.table("silver_replay_header") \
    .filter("account_id != '' AND player_nickname != ''") \
    .select("replay_id", "account_id", "player_nickname", "map_uid") \
    .show(20, truncate=False)
