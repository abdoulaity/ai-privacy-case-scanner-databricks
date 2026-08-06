### 1- OVERVIEW
Business Goals (4a — desired inputs/outputs of the AI pipeline)
High-Level Architecture Diagram (text/description for now)
Technology Stack
Data Source (CourtListener API)
Automation / Ingestion (GitHub Actions)
Compute & Storage (Databricks: Delta, GraphFrames, Unity Catalog)
Serving (Databricks Model Serving / FastAPI on Railway)
Frontend (Portfolio chatbot UI)
Data Model
Dockets / Clusters / Opinions relationship
Citation graph (cites field)
Pipeline Design (4b — chain components)
Ingestion (Search API → Case Law APIs)
Storage (Delta tables)
Processing (Spark filtering, GraphFrames)
RAG / Model layer
Serving endpoint
Secrets & Security
GitHub Secrets decision (vs. Databricks Secret Scope)
.gitignore policy
Key Technical Decisions & Trade-offs
Why GitHub Actions instead of Databricks-native fetching (free-edition egress limitation)
Ingest-broad, filter-downstream philosophy
REST API structure (Dockets/Clusters/Opinions as separate resources)
Open Questions / Future Work


### 🛠️ Technology Stack & Architecture

| Layer / Component | Technology Used | Purpose & Data Flow | Next Step |
| :--- | :--- | :--- | :--- |
| **Frontend** | React | User Interface & Client-side rendering | → Sends API requests |
| **Backend API** | Node.js | Business logic, Routing, Auth | → Queries Database / Cache |
| **Database** | PostgreSQL | Relational storage for persistent data | 💾 Final storage |
| **Caching Layer**| Redis | Session management & fast data retrieval| ⚡ Low-latency response |
