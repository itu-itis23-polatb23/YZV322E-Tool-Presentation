# Polars: A Lightning-Fast DataFrame Library for Data Engineering

> **YZV 322E — Applied Data Engineering | Spring 2026**
> Individual Tool Presentation Assignment | ITU Department of AI & Data Engineering

A working demo of **Polars** — a high-performance, Rust-based DataFrame library —
shown in the context of a real PostgreSQL ETL pipeline, with **pgAdmin** for
visual inspection of the loaded data.

The repository ships with two main entry points:

1. **`notebooks/polars_showcase.ipynb`** — interactive JupyterLab notebook that
   walks through every Polars feature (Postgres I/O, eager API, lazy API + query
   plan, streaming, benchmarks). Use this for the recorded demo.
2. **`src/etl_pipeline.py`** — production-style end-to-end ETL pipeline:
   Extract from Postgres → Transform with the lazy API → Load back to Postgres → Verify.

---

## 1. What is this tool?

[**Polars**](https://pola.rs) is an analytical query engine and DataFrame library
written in Rust, with a Python API that feels familiar if you know Pandas.
It uses **Apache Arrow** as its in-memory format, runs every operation in **parallel**
across CPU cores by default, supports **lazy evaluation** with a query optimizer,
and has a **streaming engine** for datasets larger than RAM. It is typically
**5–30× faster than Pandas** on real-world ETL workloads.

This repository demonstrates Polars in a realistic data-engineering pipeline:
read NYC Yellow Taxi data from **PostgreSQL** (a core course tool), transform it
with the lazy API, and write results back — all containerised with **Docker** (also a core course tool).

---

## 2. Prerequisites

| Requirement | Version |
| --- | --- |
| Docker Engine | ≥ 24.0 |
| Docker Compose | ≥ 2.20 (built into modern Docker) |
| Disk space | ~1 GB (for one month of taxi data + Postgres) |
| Internet | Required to download Docker images and the taxi dataset |

Everything else is inside Docker Container, so no local Python install needed.

To check your setup:

```bash
docker --version            # should print 24.0 or higher
docker compose version      # should print v2.20 or higher
```

---

## 3. Installation

```bash
# 1. Clone the repository
git clone https://github.com/itu-itis23-polatb23/YZV322E-Tool-Presentation.git
cd polars-demo

# 2. Copy the example env file (defaults work out of the box)
cp .env.example .env

# 3. Build the images and start all four services (Postgres, app, Jupyter, pgAdmin)
docker compose up -d --build

# 4. Wait ~10 seconds for Postgres to finish initialising, then verify
docker compose ps
# Expected: postgres, app, jupyter, and pgadmin all "running"
```

The `taxi_trips` table is created automatically the first time Postgres starts
(via `sql/01_schema.sql`).

---

## 4. Running the demo

### Step 1 — Load NYC Taxi data into Postgres

```bash
# Loads ~3M rows (Yellow Taxi, January 2024, ~50 MB Parquet)
docker compose exec app python data/load_taxi_data.py
```

Expected output (truncated):

```
[init] target: postgresql://polars:***@postgres:5432/taxi
[get ] https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
yellow_tripdata_2024-01.parquet: 100%|███████| 47.7M/47.7M [00:08<00:00, 5.6MB/s]
[read] yellow_tripdata_2024-01.parquet
[read] 2,964,624 rows in 0.4s
[load] writing to Postgres...
[load] done in 22.6s
[ok  ] loaded 2,964,624 rows into taxi_trips
```

### Step 2 — Open the showcase notebook

JupyterLab is already running inside the `jupyter` container. Open it in your browser:

🔗 **<http://localhost:8888/lab/tree/notebooks/polars_showcase.ipynb>**

When prompted for a token, paste the value of `JUPYTER_TOKEN` from your `.env`
file (default: `polars`).

The notebook walks through:

1. **Reading from PostgreSQL** — Pandas vs Polars+connectorx
2. **Eager API** — method chaining and expressions
3. **Lazy API** — query plan optimization (the wow moment)
4. **Streaming engine** — larger-than-RAM data
5. **Benchmark** — Pandas vs Polars on identical operations, with a chart

Run cells top-to-bottom with `Shift+Enter`. The whole notebook executes in
under 30 seconds on a typical laptop after data is loaded.

### Step 3 — Run the production ETL pipeline

```bash
docker compose exec app python src/elt_pipeline.py
```

This script demonstrates the canonical Extract → Transform → Load → Verify
pattern using the lazy API. It builds a `taxi_hourly_summary` table in Postgres
that aggregates trips by (zone, hour, day-of-week).

Expected output (truncated):

```
12:34:56 [INFO] ╔══════════════════════════════════════════════════════════════╗
12:34:56 [INFO] ║  Polars ETL Pipeline                                         ║
12:34:56 [INFO] ║  source:  taxi_trips                                         ║
12:34:56 [INFO] ║  target:  taxi_hourly_summary                                ║
12:34:56 [INFO] ╚══════════════════════════════════════════════════════════════╝
12:34:56 [INFO] ━━━ EXTRACT — read raw trips from Postgres
12:34:56 [INFO]     extracted 2,964,624 rows (152 MB in Arrow memory)
12:34:56 [INFO]     done in 4.12s
12:35:00 [INFO] ━━━ TRANSFORM — clean + enrich + aggregate (lazy)
12:35:00 [INFO]     optimized query plan:
                    [... query plan with PROJECT and SELECTION pushdowns ...]
12:35:00 [INFO]     transformed → 47,213 aggregated rows (2.8 MB)
12:35:00 [INFO]     done in 0.84s
12:35:01 [INFO] ━━━ LOAD — write summary back to Postgres
12:35:01 [INFO]     wrote 47,213 rows to taxi_hourly_summary
12:35:03 [INFO]     done in 2.10s
12:35:03 [INFO] ━━━ VERIFY — sanity-check the loaded table
12:35:03 [INFO]     row count    : 47,213
12:35:03 [INFO]     time range   : 2024-01-01 00:00:00 → 2024-01-31 23:00:00
12:35:03 [INFO]     unique zones : 261
12:35:03 [INFO] ━━━ pipeline finished in 7.42s
```

### Step 4 — Inspect the result in pgAdmin

pgAdmin is already running at <http://localhost:5050>. Log in with the credentials
from your `.env` file (defaults: `admin@polars.demo` / `polars_demo_2026`).

The Postgres server is **pre-configured** — you should see "Polars Demo - Postgres"
in the server tree on the left immediately after login. Click it to expand, then
navigate to:

```
Polars Demo - Postgres ▸ Databases ▸ taxi ▸ Schemas ▸ public ▸ Tables
```

You should see two tables:

- `taxi_trips` — raw input loaded from NYC TLC parquet
- `taxi_hourly_summary` — output produced by the ETL pipeline

Right-click `taxi_hourly_summary` → **View/Edit Data → First 100 Rows** to see
the aggregated output. You can also open the **Query Tool** (top toolbar) to run
ad-hoc SQL against the table.

If you'd rather use the command line:

### Optional — load more months for the streaming demo

```bash
docker compose exec app python data/load_taxi_data.py --month 2024-02 --append
docker compose exec app python data/load_taxi_data.py --month 2024-03 --append
```

The streaming section in the notebook will then process all three months at
once. Open `docker stats` in another terminal during the streaming cell to
confirm RAM stays flat.

---

## 5. Repository structure

```
polars-demo/
├── README.md                      # this file
├── docker-compose.yml             # Postgres + app + Jupyter + pgAdmin
├── Dockerfile                     # Python 3.12 + Polars 1.40 + Pandas + Jupyter
├── requirements.txt               # pinned Python dependencies
├── .env.example                   # default credentials (Postgres + Jupyter)
├── .gitignore
├── sql/
│   └── 01_schema.sql              # taxi_trips table — auto-runs on first boot
├── pgadmin/
│   ├── servers.json               # pre-configured Postgres connection
│   └── pgpass                     # pgAdmin password file
├── data/
│   └── load_taxi_data.py          # downloads + loads NYC TLC parquet → Postgres
├── notebooks/
│   └── polars_showcase.ipynb      # ★ feature walkthrough (use for the demo)
├── src/
    └── elt_pipeline.py            # ★ end-to-end ETL pipeline
```

---

## 6. Troubleshooting

**`relation "taxi_trips" does not exist`** — Postgres only runs init scripts on
*first* boot. If you need to reset:
```bash
docker compose down -v   # ⚠ deletes the volume
docker compose up -d
```

**Port 8888 already in use** — change the host port in `docker-compose.yml`
(e.g. `"8889:8888"`) and visit `http://localhost:8889`.

**pgAdmin asks me to add a server / can't connect** — the server connection is
pre-configured but only injected on **first** start of the pgadmin container.
If you changed credentials after the container was created, reset it with:
```bash
docker compose rm -sf pgadmin
docker volume rm polars-demo_pgadmin_data
docker compose up -d pgadmin
```

**Out of memory on streaming demo** — increase Docker's memory limit in
Docker Desktop → Settings → Resources, or load fewer months.

---

## 7. References

1. Polars official documentation — <https://docs.pola.rs/>
2. Polars GitHub — <https://github.com/pola-rs/polars>
3. Apache Arrow specification — <https://arrow.apache.org/>
4. connectorx — Rust-based parallel database reader: <https://github.com/sfu-db/connector-x>
5. Ritchie Vink, "I wrote one of the fastest DataFrame libraries" (2021) —
   <https://www.ritchievink.com/blog/2021/02/28/i-wrote-one-of-the-fastest-dataframe-libraries/>
6. NYC Taxi & Limousine Commission — Trip Record Data:
   <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

---

## 8. AI Usage Disclosure

---

## AI Tool Usage Disclosure

In line with academic integrity and transparent development practices, AI assistants were used during this project for the following auxiliary tasks:

- **Docker Compose configuration** — drafting and refining the `docker-compose.yml` file.
- **Code scaffolding and debugging** — generating boilerplate templates and assisting with troubleshooting.
- **Benchmarking notebook** — structuring the Jupyter notebook used for model benchmarking and result comparison.
- **README documentation** — drafting and validating the structure of this README file.

All algorithmic design, feature engineering decisions, model selection, hyperparameter tuning, and final implementation choices were made by the author.

---
