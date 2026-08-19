import os
import sys
import json
import requests

# --- Setup: Credentials, Auth Headers, API Base URL, and Output Folder ---
api_key = os.environ.get("COURT_LISTENER_KEY")
if not api_key:
    print("ERROR: COURT_LISTENER_KEY environment variable not set.")
    sys.exit(1)

headers = {"Authorization": f"Token {api_key}"}
base_url = "https://www.courtlistener.com/api/rest/v4"
os.makedirs("data/api_data_examples", exist_ok=True)

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
params = {"q": query, "type": "o"}
opinions_data = []  # will collect every fetched opinion, across all pages and all cases

# --- Step 3: Loop Through Every Page of Search Results (Pagination) ---
while url:
    try:
        response_multi = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=10
        )
        response_multi.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("ERROR: Multi-keyword search failed ->", e)
        sys.exit(1)

    page_data = response_multi.json()
    print("Fetched page with", len(page_data["results"]), "results")

    # For each case in this page, fetch the full data of every opinion it contains
    # (Not just the first one, but rather: majority, dissent, concurrence, etc.)
    for result in page_data["results"]:
        for opinion in result["opinions"]:
            opinion_id = opinion["id"]
            try:
                opinion_response = requests.get(
                    f"{base_url}/opinions/{opinion_id}/",
                    headers=headers,
                    timeout=10
                )
                opinion_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"ERROR: Opinion fetch failed for id {opinion_id} ->", e)
                continue

            opinions_data.append(opinion_response.json())
            time.sleep(0.5)  # Pace individual opinion fetches to avoid rate-limit throttling

    # Move to the next page; None means the query params are already baked into the "next" URL
    url = page_data["next"]
    params = None
    time.sleep(0.5)  # pace page fetches too

# Save every opinion collected across all pages into one file
with open("data/api_data_ingestion/opinions.json", "w") as f:
    json.dump(opinions_data, f, indent=2)

print(f"Saved {len(opinions_data)} opinions to opinions.json")