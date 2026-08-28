import os
import sys
import json
import time
import requests
from datetime import datetime

# --- Setup: Credentials, Auth Headers, API Base URL, and Output Folder ---
api_key = os.environ.get("COURT_LISTENER_KEY")
if not api_key:
    print("ERROR: COURT_LISTENER_KEY environment variable not set.")
    sys.exit(1)

headers = {"Authorization": f"Token {api_key}"}
base_url = "https://www.courtlistener.com/api/rest/v4"
os.makedirs("data/api_data_examples", exist_ok=True)

# --- Read existing checkpoint, if any ---
checkpoint_path = "data/checkpoint/checkpoint.json"
os.makedirs("data/checkpoint", exist_ok=True)

if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "r") as f:
        checkpoint = json.load(f)
    filed_after = checkpoint["last_date_filed"]
    print("Checkpoint found. Running incremental from:", filed_after)
else:
    checkpoint = None
    filed_after = "2016-12-31"  # backfill default, first-ever run
    print("No checkpoint found. Running backfill from:", filed_after)

filed_before = datetime.now().strftime("%Y-%m-%d")

# --- Step 1: Basic Connectivity Test ---
try:
    response = requests.get(
        f"{base_url}/search/",
        headers=headers,
        params={"q": "test", "type": "o"},
        timeout=10
    )
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print("ERROR: Connectivity test failed ->", e)
    sys.exit(1)

print("Connectivity Test: Passed")

# --- Step 2: Multi-keyword Search Query ---
query = '"data privacy" OR "data protection" OR "GDPR" OR "CCPA" OR "personal information" OR "personal data" OR "data breach"'

url = f"{base_url}/search/"
params = {"q": query, "type": "o", "filed_after": filed_after, "filed_before": filed_before}
opinions_data = []  # will collect every fetched opinion, across all pages and all cases

# --- Step 3: Loop Through Every Page of Search Results (Pagination) ---
while url:
    for attempt in range(5):
        try:
            response_multi = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=10
            )
            if response_multi.status_code == 429:
                wait = int(response_multi.headers.get("Retry-After", 60))
                print(f"Rate limited on search. Waiting {wait}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            response_multi.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print("ERROR: Multi-keyword search failed ->", e)
            sys.exit(1)
    else:
        print("ERROR: Search request failed after max retries.")
        sys.exit(1)

    page_data = response_multi.json()
    print("Fetched page with", len(page_data["results"]), "results")

    # For each case in this page, fetch the full data of every opinion it contains
    # (Not just the first one, but rather: majority, dissent, concurrence, etc.)
    for result in page_data["results"]:
        for opinion in result["opinions"]:
            opinion_id = opinion["id"]

            for attempt in range(5):
                try:
                    opinion_response = requests.get(
                        f"{base_url}/opinions/{opinion_id}/",
                        headers=headers,
                        timeout=10
                    )
                    if opinion_response.status_code == 429:
                        wait = int(opinion_response.headers.get("Retry-After", 60))
                        print(f"Rate limited on opinion {opinion_id}. Waiting {wait}s (attempt {attempt+1}/5)...")
                        time.sleep(wait)
                        continue
                    opinion_response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    print(f"ERROR: Opinion fetch failed for id {opinion_id} ->", e)
                    break
            else:
                print(f"ERROR: Opinion {opinion_id} failed after max retries. Skipping.")
                continue

            if opinion_response.status_code != 200:
                continue

            opinions_data.append(opinion_response.json())
            time.sleep(2)  # pace individual opinion fetches to avoid rate-limit throttling
            opinion_date = result.get("dateFiled")
            if opinion_date and (checkpoint is None or opinion_date > filed_after):
                filed_after = opinion_date  # will become the new checkpoint value

    # Move to the next page; None means the query params are already baked into the "next" URL
    url = page_data["next"]
    params = None
    time.sleep(2)  # pace page fetches too
    
# Save every opinion collected across all pages into one file
with open("data/api_data_ingestion/opinions.json", "w") as f:
    json.dump(opinions_data, f, indent=2)

print(f"Saved {len(opinions_data)} opinions to opinions.json")

# --- Step 4: Write updated checkpoint ---
new_checkpoint = {
    "last_date_filed": filed_after,
    "last_run": datetime.now().isoformat(),
    "total_opinions_fetched": len(opinions_data)
}

with open(checkpoint_path, "w") as f:
    json.dump(new_checkpoint, f, indent=2)

print("Checkpoint updated:", new_checkpoint)