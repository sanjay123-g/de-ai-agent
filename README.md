# Cloud-Cube FP&A AI

> A local, production-grade Financial Planning & Analysis engine powered by dbt, DuckDB, and AI.
> Query your financials in plain English. Detect anomalies automatically. Zero cloud costs.

![dbt](https://img.shields.io/badge/dbt-1.8.0-orange)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10.3-yellow)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-green)
![Python](https://img.shields.io/badge/Python-3.11.9-blue)

---

## What It Does

- **dbt pipeline** — transforms raw financial transactions through staging and marts layers with automated data quality tests
- **AI agent** — type a question in plain English, get SQL executed against a real database instantly
- **Anomaly detection** — automatically flags duplicate transactions, budget overruns, and unmapped accounts
- **Live macro benchmarks** — pulls real GDP and inflation data from the World Bank API
- **Pivot table dashboard** — drag-and-drop financial explorer in the browser

---

## Quickstart

Clone and setup:
```bash
git clone https://github.com/sanjay123-g/cloud-cube-fpa-ai.git
cd cloud-cube-fpa-ai
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

Build the database:
```bash
cd fpa_dbt_project
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..
```

Start the API server:
```bash
python Excel_bridge/mock_excel_addin.py
```

Open the dashboard:
```bash
open frontend_dashboard/index.html
```

---

## Stack

| Layer          | Tool                                 |
| -------------- | ------------------------------------- |
| Warehouse      | DuckDB (local) → Snowflake (roadmap)  |
| Transformation | dbt Core 1.8                          |
| AI Agent       | Groq API — llama-3.3-70b (free tier)  |
| API            | FastAPI + uvicorn                     |
| Frontend       | WebDataRocks pivot + vanilla JS       |
| External Data  | World Bank API (free, no key)         |

---

## Project Structure

```
fpa_dbt_project/       dbt project (models, seeds, tests)
  models/staging/      stg_ledger — type casting layer
  models/marts/        fct_ledger — aggregated metrics
  seeds/               raw_ledger.csv source data
backend_agent/         Python AI agent + anomaly detection
Excel_bridge/          FastAPI server (3 endpoints)
frontend_dashboard/    Dark theme pivot table UI
portfolio_assets/      Case study documentation
```

---

## API Endpoints

| Method | Route      | Description                          |
| ------ | ---------- | ------------------------------------- |
| GET    | /          | Health check                          |
| POST   | /api/query | Natural language to SQL to results    |
| GET    | /api/audit | Full anomaly detection report         |
| GET    | /api/data  | Raw fct_ledger data for pivot table   |

---

## Data Quality

17 automated dbt tests run on every build:
- Uniqueness and not_null checks on `transaction_id`
- Accepted-values check on `scenario` (Actual/Budget only)
- 3 duplicate transaction IDs caught by the uniqueness test
- IT department budget overrun at 190% caught by the anomaly detector

---

## Roadmap

- Phase 6 — Real public dataset + star schema
- Phase 7 — Snowflake migration
- Phase 8 — Dagster orchestration
- Phase 9 — GitHub Actions CI/CD
- Phase 10 — UI redesign with charts and export
- Phase 11 — Interactive CLI for dynamic querying

---

## Author

Sanjay Gopinath — Analytics Engineer (8+ years) transitioning into AI Engineering, building production AI systems on top of modern data infrastructure.
Stack: dbt · Snowflake · Python · FastAPI · LangChain · Claude API
[LinkedIn](https://www.linkedin.com/in/gopinathsanjay/) · [Other projects](https://github.com/sanjay123-g)
