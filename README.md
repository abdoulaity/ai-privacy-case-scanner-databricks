# AI PRIVACY CASE SCANER

#### Overview
A data pipeline and AI agent for researching and summarizing data protection case law, combining CourtListener's case law data with Databricks-based RAG and citation graph analysis.

#### Business Goals
- Enable **researchers and data protection consultants** to quickly find and understand relevant case law
- Summarize opinions and surface citation relationships between cases
- Support research workflows with both semantic search and citation-authority signals (e.g. most-cited cases)

#### Technical Stack

| Layer | Technology |
|---|---|
| Data source | CourtListener REST API v4 |
| Ingestion / automation | GitHub Actions |
| Compute & storage | Databricks (Delta Lake, Spark, GraphFrames, Unity Catalog) |
| Citation graph | GraphFrames (PageRank, citation network) |
| Model serving | Databricks Model Serving |
| Backend API | FastAPI, hosted on Railway |
| Frontend | React + Tailwind + ShadCN (portfolio chatbot UI) |

#### Setup
1. Create a free [CourtListener account](https://www.courtlistener.com/sign-in/) and generate an API token
2. Add the token as a GitHub Secret: `COURTLISTENER_TOKEN`
3. Clone this repo
4. Run the exploration script locally or via GitHub Actions (`workflow_dispatch`)
5. See `ARCHITECTURE.md` for full pipeline design and technical decisions