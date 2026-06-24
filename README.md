# Fleet & Supply Chain Intelligence Platform

End-to-end Databricks Lakehouse project: real-time + batch logistics
analytics for a simulated truck fleet operating on real Maharashtra
(India) highway corridors.

## What this project demonstrates

- **Medallion architecture** (Bronze → Silver → Gold) on Delta Lake
- **Batch ingestion** via Auto Loader (schema evolution, incremental loads)
- **Streaming ingestion** via Structured Streaming (watermarking, checkpoints)
- **CDC / SCD Type 2** on slowly-changing dimension data
- **Data quality enforcement** with quarantine logic
- **Orchestration** via Databricks Workflows
- **Governance** via Unity Catalog (column masking, row-level security, lineage)
- **CI/CD across DEV → UAT → PROD** using Databricks Asset Bundles + GitHub Actions
- **BI serving layer** via Databricks SQL dashboards
- *(optional)* Predictive maintenance ML model via MLflow

## Architecture

See `docs/architecture_diagram.png` (added in a later phase).

```
Real-time sources ──┐                    ┌─→ Gold (streaming aggregates)
  (GPS/sensor/        ├─→ Bronze ─→ Silver ─┤
   delivery events)  ─┘                    └─→ Gold (batch rollups)
Batch sources ───────┘
  (manifests/invoices/
   maintenance/master)
```

## Real vs. synthetic data

This project deliberately mixes real and synthetic data:
- **Real:** highway corridors, distances, speed limits, city coordinates,
  vehicle operating parameters (see `data_generator/README.md` for sources/citations)
- **Synthetic:** the fleet, drivers, shipments, telemetry, and all
  transactional data (generated via `Faker` + a custom simulation engine,
  since no public dataset exists for a real company's internal logistics data)

## Repo structure

```
├── data_generator/      # synthetic data generation scripts + sample output
├── setup/               # one-time environment setup scripts (Unity Catalog, etc.)
├── notebooks/
│   ├── 00_setup/
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_gold/
│   └── 04_dashboards/
├── pipelines/           # Lakeflow/DLT pipeline definitions
├── resources/           # Databricks Asset Bundle resource definitions (jobs, pipelines)
├── tests/               # data quality / row-count / schema validation tests
├── .github/workflows/   # CI/CD pipeline definitions (GitHub Actions)
└── docs/                # architecture diagrams, progress log, design notes
```

## Environments

| Environment | Catalog | Purpose |
|---|---|---|
| DEV | `fleet_dev` | Active development, full data visibility |
| UAT | `fleet_uat` | Validation/testing, masked PII, mirrors PROD |
| PROD | `fleet_prod` | Production, strictest governance |

## Build log

See `docs/progress_log.md` for a session-by-session build journal.

## Status

🚧 In progress. Currently on: **Phase 0 — Foundations & Environment Setup**
