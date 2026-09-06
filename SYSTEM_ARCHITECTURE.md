# SYSTEM ARCHITECTURE

#### 1. Technical Stack

| LAYER | TECHNOLOGY | STATUS | 
|---|---|---|
| Data source | CourtListener REST API v4 | Implemented |
| Ingestion & Automation | GitHub Actions | Implemented |
| Compute & Storage | Databricks (Delta Lake, Spark, GraphFrames, Unity Catalog) | Planned |
| Citation Graph | GraphFrames (PageRank, Citation Network) | Planned |
| Model serving | Databricks Model Serving | Planned |
| Backend API | FastAPI, hosted on Railway | Planned |
| Frontend | Chatbot UI: React + Tailwind + ShadCN | Planned |

#### 2. Data Source: CourtListener REST API v4

**Note**: I will revert back to this section soon. Since I update the docs as I progress in the coding part, sometimes I need **prioritize** another section to avoid forgetting an important architecture decision. That's the case for `3. Ingestion and Automation: Github Actions`. 

#### 3. Ingestion and Automation: Github Actions

**Databricks' free edition** blocks outbound internet access, so all external API calls happen in **GitHub Actions** instead, with results pushed into a **Databricks Unity Catalog Volume** via the **Files API**. **Three** separate workflows handle this, each with a distinct purpose:

>**API Data Examples**: a one-off exploratory workflow that fetches a **single search result** and a **single opinion** from **CourtListener**, saving them as reference JSON files. Used early in the project to inspect the **API's response schema** before designing the ingestion pipeline.

>**Historical Backfill**: populates the initial corpus by paginating through a fixed date range (`2020-12-31 to 2026-06-30`), fetching every **matching opinion**. Triggered **hourly** via cron until completion, this workflow processes a capped number of pages per run to respect **CourtListener's rate limits**, using a shared **checkpoint file** to resume exactly where the previous run left off.

>**Incremental Ingestion**: once backfill completes, this workflow takes over on a slower, recurring schedule (` tri-annual`), fetching only cases filed since the last successful run. This keeps the corpus current without re-processing already-ingested data.

All **three workflows** authenticate to **CourtListener** via a token stored as a **GitHub Secret**, and to **Databricks** via a separately scoped **Personal Access Token** (requiring the files API scope) used to upload results to the **Volume**.