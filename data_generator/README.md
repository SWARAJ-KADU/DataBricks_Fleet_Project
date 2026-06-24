# Fleet Data Generator

Generates the synthetic dataset for the Fleet & Supply Chain Intelligence
Databricks project, grounded in real Maharashtra (India) highway geography
and real vehicle operating parameters.

## Folder contents

| File | Purpose |
|---|---|
| `reference_data.py` | REAL reference data: cities, highway distances, speed limits, vehicle specs |
| `geo_utils.py` | Math engine: interpolates a position along a real route |
| `fleet_simulator.py` | Generates the synthetic fleet (trucks + drivers via Faker) |
| `batch_generator.py` | Generates historical batch files (45-60 days) |
| `streaming_producer.py` | Generates live streaming-style events (GPS/sensor/delivery) |

## Setup

```bash
pip install -r requirements.txt
```

## Usage

### 1. Generate historical batch data (run once, before building Bronze)

```bash
python batch_generator.py --days 45 --start-date 2026-05-01 --output ./output --num-trucks 12
```

Produces, under `./output/`:
- `shipment_manifests/YYYY-MM-DD.json` (newline-delimited JSON)
- `invoices/YYYY-MM-DD.csv`
- `maintenance_logs/YYYY-MM-DD.csv` (sparse — not every truck every day)
- `driver_vehicle_master/YYYY-MM-DD.csv` (daily snapshot — SCD2 source)

Upload the `output/` folder to your Databricks workspace (e.g. via the
Catalog > Upload UI, or `%fs cp` from a Databricks Repos-synced copy) as
your "batch landing zone" for Auto Loader.

### 2. Run the streaming producer (run live during a Databricks session)

```bash
python streaming_producer.py --mode file --output ./stream_output --duration-minutes 15 --tick-seconds 10 --num-trucks 12
```

Produces, under `./stream_output/`:
- `gps_events/batch_<timestamp>_<n>.json`
- `sensor_events/batch_<timestamp>_<n>.json`
- `delivery_events/batch_<timestamp>_<n>.json` (sparse)

Point a Structured Streaming `readStream` (file source / Auto Loader) at
each subfolder. Run this script from a terminal alongside your Databricks
session, writing into a path your workspace can read (e.g. a synced/mounted
folder), or adapt it to write directly via `dbutils.fs` if running inside
a Databricks notebook itself.

Stop early any time with Ctrl+C — each batch is a complete, valid file.

## Real data sources used in `reference_data.py`

All real-world facts below are cited with their source so you (or an
interviewer) can verify them:

| Fact used in code | Real value | Source |
|---|---|---|
| Mumbai-Pune Expressway distance | 94.5 km | [Wikipedia: Mumbai–Pune Expressway](https://en.wikipedia.org/wiki/Mumbai%E2%80%93Pune_Expressway) |
| Mumbai-Pune truck speed limit | 80 km/h (flat), 60 km/h (ghat/hilly) | [Wikipedia: Mumbai–Pune Expressway](https://en.wikipedia.org/wiki/Mumbai%E2%80%93Pune_Expressway) |
| Mumbai-Nashik Expressway distance | 150 km | [Maps of India: Mumbai Nashik Expressway](https://www.mapsofindia.com/roads/expressway/mumbai-nashik.html) |
| NH 60 (Pune-Nashik-Dhule corridor) | 360.6-398.2 km total length | [Wikipedia: National Highway 60 (India)](https://en.wikipedia.org/wiki/National_Highway_60_(India)) |
| Maharashtra national highway network | 18,462 km across 102 highways, connecting Mumbai, Pune, Nagpur, Nashik | [Grokipedia: List of national highways in India by state](https://grokipedia.com/page/List_of_national_highways_in_India_by_state) |
| City coordinates (Mumbai, Pune, Nashik, Aurangabad) | Standard WGS84 lat/long | Public geographic coordinates (e.g. [latlong.net](https://www.latlong.net/), Wikipedia city infoboxes) |
| Maharashtra RTO plate prefixes (MH12, MH14, MH20, MH04) | Real registration district codes | [Parivahan Sewa (Govt. of India vehicle registration)](https://parivahan.gov.in/) |

**Note on vehicle specs:** fuel tank capacity, fuel economy, and engine/cargo
temperature ranges in `VEHICLE_SPECS` are *representative* figures for
medium commercial trucks and refrigerated vans common in Indian freight
(e.g. Tata LPT-series, Ashok Leyland) rather than scraped from a single
official spec sheet — described in code comments as "representative" for
this reason. If you want to tighten this further, manufacturer spec pages
(e.g. [Tata Motors trucks](https://www.tatamotors.com/), [Ashok Leyland](https://www.ashokleyland.com/))
publish official tank capacity and mileage figures you could substitute in.

## Reproducibility

- `batch_generator.py` and `fleet_simulator.py` use fixed random seeds
  (`Faker.seed(7)`, `random.seed(7)` / `seed(42)`) so historical batch data
  is reproducible across runs — useful if you need to regenerate after a
  Vocareum/lab reset and want identical data.
- `streaming_producer.py` intentionally uses real randomness (no fixed
  seed) since it's meant to simulate a live, non-reproducible feed each
  session — which is realistic streaming behavior.

## Known simplifications (worth mentioning proactively in interviews)

- GPS interpolation is straight-line (great-circle) between real city
  waypoints, not snapped to actual road curvature. A production version
  would call a routing API (e.g. OSRM, Google Roads API) for true road-
  following coordinates.
- The fleet "bounces" back and forth along its route rather than running
  discrete one-way trips with proper trip lifecycle management — a
  reasonable simplification for generating continuous streaming volume
  during short demo sessions.
