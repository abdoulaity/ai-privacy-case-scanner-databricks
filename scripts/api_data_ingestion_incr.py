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

MAX_PAGES_PER_RUN = 2 # small cap: about 40 opinions per run keeps one run well under the 125 requests/day limit

# --- Retry Settings (shared by the search call and the opinion calls) ---
MAX_ATTEMPTS = 3            # attempts per request before giving up
REQUEST_TIMEOUT = 30        # seconds to wait for CourtListener to answer
RETRY_PAUSE_SECONDS = 30    # pause before retrying a timeout, a connection error, or a server error
LONG_WAIT_SECONDS = 300     # if a 429 asks us to wait longer than this, the quota is gone: I need to stop the run

# --- Retry Helper: one GET request with retries ---
def get_with_retries(request_url, request_params, label):
    """
    Returns (response, status), where status is:
      "ok"     -> got a 200 response
      "quota"  -> a 429 asked us to wait longer than LONG_WAIT_SECONDS (quota probably used up)
      "failed" -> gave up (all attempts used, or an error that retrying won't fix)
    """
    for attempt in range(MAX_ATTEMPTS):
        is_last_attempt = (attempt == MAX_ATTEMPTS - 1)

        try:
            resp = requests.get(
                request_url,
                headers=headers,
                params=request_params,
                timeout=REQUEST_TIMEOUT
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Momentary network problem: worth another try
            print(f"Transient error on {label}: {e} (attempt {attempt+1}/{MAX_ATTEMPTS})")
            if not is_last_attempt:
                time.sleep(RETRY_PAUSE_SECONDS)
            continue
        except requests.exceptions.RequestException as e:
            # Any other request error: I guess retrying won't help here
            print(f"ERROR on {label} (not retryable): {e}")
            return None, "failed"

        if resp.status_code == 200:
            return resp, "ok"

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            if wait > LONG_WAIT_SECONDS:
                print(f"Rate limited on {label}: asked to wait {wait}s, the quota is probably used up.")
                return None, "quota"
            print(f"Rate limited on {label}. Waiting {wait}s (attempt {attempt+1}/{MAX_ATTEMPTS})...")
            if not is_last_attempt:
                time.sleep(wait)  # no point sleeping after the last attempt
            continue

        if resp.status_code >= 500:
            # CourtListener-side hiccup (the likes of 502 Bad Gateway): worth another try
            print(f"Server error {resp.status_code} on {label} (attempt {attempt+1}/{MAX_ATTEMPTS})")
            if not is_last_attempt:
                time.sleep(RETRY_PAUSE_SECONDS)
            continue

        # Anything else (401, 404, ...): retrying won't fix it
        print(f"ERROR on {label}: HTTP {resp.status_code} (not retryable)")
        return None, "failed"

    print(f"ERROR: {label} failed after {MAX_ATTEMPTS} attempts.")
    return None, "failed"

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
skipped_ids = []     # opinions that could not be fetched, listed in the log at the end
pages_processed = 0
stop_early = False   # becomes True if the quota runs out or the search call keeps failing

while url and pages_processed < MAX_PAGES_PER_RUN:
    response_multi, status = get_with_retries(url, params, "search")

    if status != "ok":
        print("Search call did not succeed. Leaving the loop and keeping what was fetched so far.")
        stop_early = True
        break

    page_data = response_multi.json()
    pages_processed += 1
    print(f"Fetched page {pages_processed}/{MAX_PAGES_PER_RUN} with", len(page_data["results"]), "results")

    for result in page_data["results"]:
        for opinion in result["opinions"]:
            opinion_id = opinion["id"]

            # Each call returns its own response (or None), so an old response can never be reused by mistake
            opinion_response, status = get_with_retries(
                f"{base_url}/opinions/{opinion_id}/",
                None,
                f"opinion {opinion_id}"
            )

            if status == "quota":
                print("Quota looks used up. Leaving the loop and keeping what was fetched so far.")
                stop_early = True
                break  # leaves the opinion loop

            if status != "ok":
                print(f"ERROR: Opinion {opinion_id} skipped after retries.")
                skipped_ids.append(opinion_id)
                continue

            opinions_data.append(opinion_response.json())
            time.sleep(2)
            opinion_date = result.get("dateFiled")
            if opinion_date and opinion_date > filed_after:
                filed_after = opinion_date

        if stop_early:
            break  # leaves the result loop

    if stop_early:
        break  # leaves the page loop

    url = page_data["next"]
    params = None
    time.sleep(2)

if skipped_ids:
    print(f"Skipped {len(skipped_ids)} opinions: {skipped_ids}")

# If nothing at all was fetched (and something went wrong), fail the run: nothing to save, checkpoint stays unchanged
if not opinions_data and (stop_early or skipped_ids):
    print("ERROR: No opinion was fetched in this run.")
    sys.exit(1)

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
    "next_page_url": None,  # always None: pages left unread in this run are dropped on purpose
    "last_run": datetime.now().isoformat(),
    "total_opinions_fetched": len(opinions_data)
}

with open(checkpoint_path, "w") as f:
    json.dump(new_checkpoint, f, indent=2)

print("Checkpoint updated:", new_checkpoint)