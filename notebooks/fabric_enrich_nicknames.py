"""
Microsoft Fabric Notebook: Enrich silver_replay_header with Player Nicknames

Reads rows from silver_replay_header where account_id is set but player_nickname
is empty, looks up the display names via the Trackmania OAuth API in batches,
and updates the table using a MERGE statement.

Prerequisites:
- A registered OAuth app at https://api.trackmania.com (gives you client_id + client_secret)
- silver_replay_header Delta table must already exist (run fabric_ghost_ingest.py first)

Parameters (edit in Cell 1):
- TM_CLIENT_ID     : Your Trackmania OAuth app Identifier
- TM_CLIENT_SECRET : Your Trackmania OAuth app Secret
"""

# ========================================
# Cell 1: Parameters
# ========================================

TM_CLIENT_ID     = ""   # Your OAuth app Identifier
TM_CLIENT_SECRET = ""   # Your OAuth app Secret

# How many account IDs to look up per API call (max 50 per API docs)
BATCH_SIZE = 50

# ========================================
# Cell 2: Authenticate with Trackmania OAuth API
# ========================================

import requests

USER_AGENT = "tm2020-gbx-parser / fabric-enrich-nicknames / contact via GitHub"

try:
    token_resp = requests.post(
        "https://api.trackmania.com/api/access_token",
        headers={"User-Agent": USER_AGENT},
        data={
            "grant_type": "client_credentials",
            "client_id": TM_CLIENT_ID,
            "client_secret": TM_CLIENT_SECRET,
        },
    )
    token_resp.raise_for_status()
except Exception as e:
    raise RuntimeError(f"OAuth token request failed — check TM_CLIENT_ID / TM_CLIENT_SECRET. Details: {e}")

access_token = token_resp.json()["access_token"]

API_HEADERS = {
    "Authorization": f"Bearer {access_token}",
    "User-Agent": USER_AGENT,
}
print("✓ OAuth token obtained")

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

DISPLAY_NAMES_URL = "https://api.trackmania.com/api/display-names"

nicknames = {}

for i in range(0, len(missing), BATCH_SIZE):
    batch = missing[i:i + BATCH_SIZE]
    try:
        response = requests.get(
            DISPLAY_NAMES_URL,
            headers=API_HEADERS,
            params=[("accountId[]", aid) for aid in batch],
        )
        response.raise_for_status()
        for account_id, display_name in response.json().items():
            nicknames[account_id] = display_name
        print(f"  Batch {i // BATCH_SIZE + 1}: looked up {len(batch)} account(s)")
    except Exception as e:
        print(f"  ✗ Batch {i // BATCH_SIZE + 1} failed: {e} — skipping")

print(f"\n✓ Resolved {len(nicknames)} nickname(s)")

# ========================================
# Cell 5: Update silver_replay_header
# ========================================

if nicknames:
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
