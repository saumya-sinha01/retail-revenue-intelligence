# 📊 Retail Revenue Intelligence

An end-to-end data engineering and analytics platform that simulates a retail business environment to solve key business challenges around revenue visibility, inventory management, and supply chain performance.

In real-world retail systems, data from orders, inventory, and logistics often exists in silos — making it difficult to measure critical metrics such as revenue loss due to stockouts, inventory availability at the time of purchase, and the impact of fulfillment delays on business performance. This project addresses these gaps by building a unified pipeline that integrates multiple data sources, applies accurate point-in-time transformations, and generates reliable, decision-ready analytics at scale.

---

## 🚀 Project Overview

This project builds a **medallion architecture (Bronze → Silver → Gold)** pipeline that:

- Ingests raw data from multiple sources (synthetic orders, PostgreSQL inventory, mock logistics API)
- Cleans, joins, and transforms data into a point-in-time accurate Silver fact table
- Generates business KPIs across revenue, risk, and supply chain dimensions
- Visualizes insights through an interactive Streamlit dashboard with live filters and trend deltas

**Scale:** 50,000 orders · 200 SKUs · 5,000 customers · 10 warehouses · 6 months of history

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────┐
│               Data Sources                  │
│  Orders (Python Generator)                  │
│  Inventory (PostgreSQL snapshots)           │
│  Logistics (FastAPI Mock Service)           │
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│            Bronze Layer (S3)                │
│  Raw JSON files partitioned by source       │
│  s3://retail-bronze/orders/                 │
│  s3://retail-bronze/inventory/              │
│  s3://retail-bronze/logistics/              │
└────────────────────┬────────────────────────┘
                     │  PySpark
                     ▼
┌─────────────────────────────────────────────┐
│            Silver Layer (S3)                │
│  Cleaned & point-in-time joined fact table  │
│  Partitioned by region                      │
│  s3://retail-silver/order_facts/            │
└────────────────────┬────────────────────────┘
                     │  PySpark
                     ▼
┌─────────────────────────────────────────────┐
│             Gold Layer (S3)                 │
│  Revenue KPIs (summary, region, channel,    │
│    month, month×region, month×channel, sku) │
│  Revenue at Risk (summary, region, sku)     │
│  Supply Chain (summary, warehouse, top SKUs)│
└────────────────────┬────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────┐
│         Streamlit Dashboard                 │
│  Sidebar filters: Month · Region            │
│  KPIs with MoM delta · Charts · Tables      │
└─────────────────────────────────────────────┘
```

**Orchestration:** Apache Airflow DAG manages the full pipeline with correct dependency ordering.

```
bronze_orders ──► bronze_logistics ──┐
bronze_inventory ────────────────────┴──► silver_order_facts ──► gold_revenue_kpis
                                                              ├──► gold_revenue_at_risk
                                                              └──► gold_supply_chain_metrics
```

---

## 🧱 Tech Stack

| Component         | Technology                        |
|-------------------|-----------------------------------|
| Data Processing   | PySpark 3.5.1                     |
| Orchestration     | Apache Airflow 2.9.3              |
| Object Storage    | LocalStack (S3 simulation)        |
| Database          | PostgreSQL 15                     |
| API Simulation    | FastAPI + Uvicorn                 |
| Visualization     | Streamlit                         |
| Containerization  | Docker + Docker Compose           |
| Language          | Python 3.12                       |

---

## 📁 Project Structure

```
retail-revenue-intelligence/
├── dags/
│   └── retail_revenue_intelligence_dag.py   # Airflow DAG (full pipeline)
├── infra/
│   ├── airflow/Dockerfile                   # Airflow + Java + PySpark image
│   ├── api/
│   │   ├── Dockerfile
│   │   ├── mock_logistics_api.py            # FastAPI mock with bulk endpoint
│   │   └── requirements.txt
│   ├── localstack/init_aws.sh               # Creates S3 buckets on startup
│   └── postgres/init.sql                    # Inventory seed data (2,000+ rows)
├── src/
│   ├── dashboard/
│   │   ├── app.py                           # Streamlit entry point + sidebar filters
│   │   ├── data_loader.py                   # S3 parquet reader
│   │   └── components/
│   │       ├── revenue.py                   # Revenue KPI charts
│   │       ├── risk.py                      # Revenue at risk charts
│   │       └── supply_chain.py              # Supply chain metrics
│   └── spark_jobs/
│       ├── bronze/
│       │   ├── bronze_orders_ingest.py      # Generates & uploads 50k orders
│       │   ├── bronze_inventory_ingest.py   # Pulls inventory from PostgreSQL
│       │   └── bronze_logistics_ingest.py   # Bulk fetches logistics from API
│       ├── silver/
│       │   └── silver_order_facts.py        # Point-in-time join, partitioned parquet
│       └── gold/
│           ├── gold_revenue_kpis.py         # Revenue aggregations (7 datasets)
│           ├── gold_revenue_at_risk.py      # Stockout risk metrics
│           └── gold_supply_chain_metrics.py # Warehouse & fulfillment metrics
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## 🔄 Data Pipeline Details

### Bronze Layer — Raw Ingestion

| Source | Details |
|--------|---------|
| **Orders** | 50,000 synthetic orders over 6 months. Weighted by region (West 35%, East 25%), channel (online 45%, store 25%), and SKU velocity (top 40 SKUs = 70% of volume). Uploaded in batches of 1,000 to `s3://retail-bronze/orders/` |
| **Inventory** | 2,000+ rows seeded in PostgreSQL (100 SKUs × 10 warehouses × 4 snapshot dates). High-velocity SKUs (001–040) intentionally seeded with low stock to produce realistic stockouts. Extracted and stored to `s3://retail-bronze/inventory/` |
| **Logistics** | Mock FastAPI service with a `/shipments/bulk` endpoint. Fetches shipment status in batches of 500 orders. 5 carriers, 5 delivery statuses with realistic weights (55% DELIVERED). Stored to `s3://retail-bronze/logistics/` |

### Silver Layer — Transformation

- Reads all three Bronze sources using PySpark
- Applies a **point-in-time inventory join**: only inventory snapshots taken before the order timestamp are considered, selecting the latest valid snapshot via a window function
- Deduplicates on `(order_id, sku)`
- Derives `order_value`, `stockout_flag`, `inventory_at_order_time`
- Writes partitioned parquet to `s3://retail-silver/order_facts/` partitioned by `region`

**Silver Fact Table Schema:**

| Column | Description |
|--------|-------------|
| order_id | Unique order identifier |
| sku | Product SKU |
| customer_id | Customer identifier |
| order_ts | Order timestamp |
| channel | Sales channel (online/store/mobile/marketplace) |
| region | Geographic region |
| quantity | Units ordered |
| price | Unit price |
| order_value | qty × unit_price |
| inventory_at_order_time | Stock on hand at order time |
| stockout_flag | True if inventory < quantity ordered |
| warehouse_id | Fulfilling warehouse |
| delivery_status | Shipment delivery status |
| carrier | Logistics carrier |
| ship_ts | Shipment timestamp |
| delivered_ts | Delivery timestamp |
| ingestion_ts | Pipeline ingestion timestamp |

### Gold Layer — KPIs

**Revenue KPIs** (`s3://retail-gold/revenue_kpis/`)
- `summary/` — overall total revenue, orders, avg order value
- `by_region/` — revenue breakdown by region
- `by_channel/` — revenue breakdown by channel
- `by_month/` — monthly revenue trend
- `by_month_region/` — monthly × region (powers filtered dashboard charts)
- `by_month_channel/` — monthly × channel
- `by_sku/` — top SKUs ranked by revenue

**Revenue at Risk** (`s3://retail-gold/revenue_at_risk/`)
- `summary/` — total revenue at risk, stockout order count
- `by_region/` — risk exposure by region
- `by_sku/` — top SKUs by revenue at risk

**Supply Chain Metrics** (`s3://retail-gold/supply_chain_metrics/`)
- `summary/` — overall stockout rate, delivery success rate
- `by_warehouse/` — per-warehouse stockout and delivery rates
- `top_stockout_skus/` — top 50 SKUs by stockout frequency

---

## 📊 Dashboard

The Streamlit dashboard connects to LocalStack S3 at `http://localhost:4566` and loads all Gold parquet datasets.

### Sidebar Filters
- **Month(s)** — multiselect across available months; all KPIs recalculate instantly
- **Region(s)** — multiselect; revenue charts filter to selected regions

### Revenue Section
- Total Revenue with **MoM delta** (▲/▼ % vs previous month)
- Total Orders and Average Order Value
- Monthly Revenue Trend (line chart)
- Revenue by Region and by Channel (bar charts, sorted descending)
- Top 20 SKUs by Revenue (table)

### Revenue at Risk Section
- Total Revenue at Risk and Stockout Order count
- Revenue at Risk by Region (bar chart)
- Top SKUs at Risk (table with revenue exposure)

### Supply Chain Section
- Total Order Lines, Stockout Rate %, Delivery Success Rate %
- Stockout Rate by Warehouse (bar chart)
- Top Stockout SKUs (table)
- Full Warehouse Detail table

---

## ⚙️ Setup & Running

### Prerequisites
- Docker Desktop (running)
- Python 3.x with pip
- Git

### Step 1 — Clone and configure

```bash
git clone https://github.com/saumya-sinha01/retail-revenue-intelligence.git
cd retail-revenue-intelligence
copy .env.example .env
```

### Step 2 — Start all services

```bash
docker-compose down -v   # wipe volumes if re-running fresh
docker-compose up --build
```

This starts:

| Service | URL |
|---------|-----|
| Airflow Webserver | http://localhost:8080 |
| LocalStack (S3) | http://localhost:4566 |
| Mock Logistics API | http://localhost:8000 |
| PostgreSQL | localhost:5432 |

Wait ~2 minutes for `airflow-init` to complete.

### Step 3 — Trigger the pipeline

1. Open **http://localhost:8080**
2. Login: `admin` / `admin`
3. Find `retail_revenue_intelligence_dag`
4. Toggle it **ON** → click **▶ Trigger**

The DAG runs all 7 tasks in the correct order. Wait for all tasks to turn green (~3–5 minutes).

### Step 4 — Run the dashboard

```bash
pip install streamlit boto3 pandas pyarrow
cd src/dashboard
streamlit run app.py
```

Open **http://localhost:8501**

---

## 🔍 Key Business Insights Demonstrated

| Question | Answer from Dashboard |
|----------|----------------------|
| How much revenue is at risk from stockouts? | $6M+ exposed across 3,300+ stockout orders |
| Which region drives the most revenue? | West (35% of orders, highest avg order value) |
| Which channel leads? | Online at 45% of volume |
| Which warehouses have stockout problems? | W01 (21.7%) and W02 (29%) — all others at 0% |
| Which SKUs need urgent restocking? | SKU026, SKU010, SKU022 — each $250k–$370k at risk |
| Is revenue growing month over month? | MoM delta shown directly on the Total Revenue metric |

---

## ⚠️ Known Limitations

- Data is synthetic — weighted to be realistic but not based on real retail patterns
- LocalStack simulates AWS S3 locally; no real cloud deployment
- Delivery success rate (~55%) is driven by mock API weights, not real carrier data
- Pipeline runs full overwrite on each trigger (no incremental processing yet)

---

## 🛠️ Challenges Solved

| Challenge | Solution |
|-----------|----------|
| Spark S3 connectivity | Configured `S3AFileSystem` with hadoop-aws + aws-java-sdk-bundle JARs |
| DAG race condition | `bronze_logistics` now depends on `bronze_orders` completing first |
| Point-in-time inventory join | Window function selects latest snapshot before each order timestamp |
| Uniform data distributions | Weighted random generation: regions, channels, SKU velocity, price by channel |
| Spark JAR download on every run | Pre-cached JARs in Airflow Dockerfile at build time |
| Dashboard crash on missing data | Graceful empty DataFrame handling with user-friendly warnings |
