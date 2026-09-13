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

databricks_token_push_volume = os.environ.get("DATABRICKS_TOKEN_PUSH_VOLUME")
databricks_workspace_url = os.environ.get("DATABRICKS_WORKSPACE_URL")
volume_path = os.environ.get("DATABRICKS_VOLUME_PATH")

if not databricks_token_push_volume or not databricks_workspace_url or not volume_path:
    print("ERROR: Databricks credentials/config not set (DATABRICKS_TOKEN_PUSH_VOLUME, DATABRICKS_WORKSPACE_URL, DATABRICKS_VOLUME_PATH).")
    sys.exit(1)

headers = {"Authorization": f"Token {api_key}"}
base_url = "https://www.courtlistener.com/api/rest/v4"
os.makedirs("data/api_data_ingestion", exist_ok=True)

MAX_PAGES_PER_RUN = 30  # incremental runs are infrequent

# --- Read Existing Checkpoint (Required for Incremental Mode) ---
checkpoint_path = "data/checkpoint/checkpoint.json"
os.makedirs("data/checkpoint", exist_ok=True)

query = '"data privacy" OR "data protection" OR "GDPR" OR "CCPA" OR "personal information" OR "personal data" OR "data breach"'
base_search_url = f"{base_url}/search/"

if not os.path.exists(checkpoint_path):
    print("ERROR: No checkpoint found. Run the historical backfill script first.")
    sys.exit(1)

with open(checkpoint_path, "r") as f:
    checkpoint = json.load(f)

if checkpoint.get("next_page_url"):
    print("ERROR: Historical backfill is not yet complete (next_page_url still set). Run the backfill script first.")
    sys.exit(1)

filed_after = checkpoint["last_date_filed"]
filed_before = datetime.now().strftime("%Y-%m-%d")
url = base_search_url
params = {"q": query, "type": "o", "filed_after": filed_after, "filed_before": filed_before}
print("Running incremental ingestion from:", filed_after, "to", filed_before)

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

# --- Step 2/3: Loop Through a Capped Number of Pages (Pagination + Batching) ---
opinions_data = []
pages_processed = 0

while url and pages_processed < MAX_PAGES_PER_RUN:
    for attempt in range(3):
        try:
            response_multi = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=10
            )
            if response_multi.status_code == 429:
                wait = int(response_multi.headers.get("Retry-After", 60))
                print(f"Rate limited on search. Waiting {wait}s (attempt {attempt+1}/3)...")
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
    pages_processed += 1
    print(f"Fetched page {pages_processed}/{MAX_PAGES_PER_RUN} with", len(page_data["results"]), "results")

    for result in page_data["results"]:
        for opinion in result["opinions"]:
            opinion_id = opinion["id"]

            for attempt in range(3):
                try:
                    opinion_response = requests.get(
                        f"{base_url}/opinions/{opinion_id}/",
                        headers=headers,
                        timeout=10
                    )
                    if opinion_response.status_code == 429:
                        wait = int(opinion_response.headers.get("Retry-After", 60))
                        print(f"Rate limited on opinion {opinion_id}. Waiting {wait}s (attempt {attempt+1}/3)...")
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
            time.sleep(2)
            opinion_date = result.get("dateFiled")
            if opinion_date and opinion_date > filed_after:
                filed_after = opinion_date

    url = page_data["next"]
    params = None
    time.sleep(2)

# Save this Run's Opinions to a Uniquely Named Local File (Staging Area before Upload)
timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
filename = f"api_data_opinions_incr_{timestamp}.json"
output_path = f"data/api_data_ingestion/{filename}"

with open(output_path, "w") as f:
    json.dump(opinions_data, f, indent=2)

print(f"Saved {len(opinions_data)} opinions locally to {output_path}")

# --- Upload this Batch's File to the Databricks Volume ---
with open(output_path, "rb") as f:
    file_bytes = f.read()

upload_url = f"{databricks_workspace_url}/api/2.0/fs/files{volume_path}/{filename}?overwrite=true"
print("Upload URL:", upload_url)

upload_headers = {
    "Authorization": f"Bearer {databricks_token_push_volume}",
    "Content-Type": "application/octet-stream"
}

try:
    upload_response = requests.put(upload_url, headers=upload_headers, data=file_bytes, timeout=60)
    upload_response.raise_for_status()
    print(f"Uploaded {filename} to Databricks Volume: {volume_path}")
except requests.exceptions.RequestException as e:
    print("ERROR: Upload to Databricks Volume failed ->", e)
    sys.exit(1)

# --- Write Updated Checkpoint ---
new_checkpoint = {
    "last_date_filed": filed_after,
    "next_page_url": url,  # normally None; if set, more incremental results exist beyond this run's cap
    "last_run": datetime.now().isoformat(),
    "total_opinions_fetched": len(opinions_data)
}

with open(checkpoint_path, "w") as f:
    json.dump(new_checkpoint, f, indent=2)

print("Checkpoint updated:", new_checkpoint)