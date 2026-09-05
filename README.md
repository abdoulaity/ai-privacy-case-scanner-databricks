# AI PRIVACY CASE SCANER

#### Overview
A data pipeline and AI agent for researching and summarizing data protection case law, combining CourtListener's case law data with Databricks-based RAG and citation graph analysis.

#### Business Goals
- Enable **data protection specialists and researchers** to quickly find and understand relevant case law
- Summarize opinions and surface citation relationships between cases
- Support research workflows with both semantic search and citation-authority signals (e.g. most-cited cases)

#### Implementation

Case law is fetched from CourtListener via a scheduled GitHub Actions pipeline which incrementally ingests new opinions using a checkpoint-based backfill/incremental system. We chose this ingestion approach for reproducibility when cloning the repos. Indeed, Databricks' free tier blocks outbound internet access. With Github Actions, Data lands in a Databricks Volume, then flows into Delta tables for processing, citation graph analysis, and RAG-based retrieval.

#### Technical Stack

| LAYER | TECHNOLOGY | STATUS | 
|---|---|---|
| Data source | CourtListener REST API v4 | Implemented |
| Ingestion & Automation | GitHub Actions | Planned |
| Compute & Storage | Databricks (Delta Lake, Spark, GraphFrames, Unity Catalog) | Planned |
| Citation Graph | GraphFrames (PageRank, Citation Network) | Planned |
| Model serving | Databricks Model Serving | Planned |
| Backend API | FastAPI, hosted on Railway | Planned |
| Frontend | Chatbot UI: React + Tailwind + ShadCN | Planned |

#### Setup
1. Create a free [CourtListener account](https://www.courtlistener.com/sign-in/) and generate an API token
2. **GitHub Secrets**: in your repo settings, add:
`COURT_LISTENER_KEY` : your CourtListener API token
`DATABRICKS_TOKEN` : a Databricks Personal Access Token
`DATABRICKS_HOST` : your Databricks workspace URL
`DATABRICKS_VOLUME_PATH` : target Unity Catalog Volume path (e.g. /Volumes/catalog/schema/volume)
3. **Databricks**: create the target Unity Catalog Volume, and a workspace user with write access to it
4. Clone the repo
5. **Run ingestion**: trigger the workflow manually (`workflow_dispatch`, via GitHub's UI or the included Databricks trigger notebook) — first run performs a historical backfill, subsequent runs fetch incrementally using a checkpoint.
6. See `ARCHITECTURE.md` for full pipeline design and technical decisions