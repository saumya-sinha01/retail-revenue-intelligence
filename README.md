# 📊 Retail Revenue & Growth Analytics (POC)

An end-to-end data engineering and analytics platform that simulates a retail business environment to solve key business challenges around revenue visibility, inventory management, and supply chain performance.

In real-world retail systems, data from orders, inventory, and logistics often exists in silos, making it difficult to measure critical metrics such as revenue loss due to stockouts, inventory availability at the time of purchase, and the impact of fulfillment delays on business performance. This project addresses these gaps by building a unified pipeline that integrates multiple data sources, applies accurate point-in-time transformations, and generates reliable, decision-ready analytics.

The platform enables stakeholders to track core KPIs such as total revenue, average order value, revenue at risk due to stockouts, and supply chain efficiency, thereby supporting data-driven decision making.

This project demonstrates a modern data stack using PySpark, Airflow, LocalStack (S3), PostgreSQL, FastAPI, and Streamlit to build scalable data pipelines and business-ready analytics.
---

## 🚀 Project Overview

This project builds a **medallion architecture (Bronze → Silver → Gold)** pipeline to:

- Ingest raw data from multiple sources  
- Clean and transform data into analytics-ready datasets  
- Generate business KPIs  
- Visualize insights through dashboards  

---

## 🏗️ Architecture
         +----------------------+
         |   Data Sources       |
         |----------------------|
         | Orders              |
         | Inventory           |
         | Logistics API       |
         +----------+----------+
                    |
                    ▼
         +----------------------+
         |   Bronze Layer       |
         |----------------------|
         | Raw JSON in S3       |
         | (LocalStack)         |
         +----------+----------+
                    |
                    ▼
         +----------------------+
         |   Silver Layer       |
         |----------------------|
         | Cleaned & Conformed  |
         | Order Facts Table    |
         +----------+----------+
                    |
                    ▼
         +----------------------+
         |   Gold Layer         |
         |----------------------|
         | Revenue KPIs         |
         | Revenue at Risk      |
         | Supply Chain Metrics |
         +----------+----------+
                    |
                    ▼
         +----------------------+
         |   Visualization      |
         |----------------------|
         | Streamlit Dashboard  |
         +----------------------+


---

## 🧱 Tech Stack

| Component        | Technology |
|-----------------|-----------|
| Data Processing | PySpark |
| Orchestration   | Apache Airflow |
| Storage         | LocalStack (S3 simulation) |
| Database        | PostgreSQL |
| API Simulation  | FastAPI |
| Visualization   | Streamlit |
| Containerization| Docker |

---

---

## 🔄 Data Pipeline Flow

### 1️⃣ Bronze Layer (Raw Ingestion)

- Ingests raw data into S3 (LocalStack)
- Sources:
  - Orders
  - Inventory
  - Logistics API (FastAPI)

**Example Output:**
s3://retail-bronze/orders/2026-xx-xx.json
---

### 2️⃣ Silver Layer (Data Transformation)

- Cleans and standardizes data  
- Applies **point-in-time joins** to match inventory at order time  
- Maintains correct **fact table grain (order_id + sku)**  

**Key Fields:**
- order_id  
- sku  
- order_value  
- inventory_at_order_time  
- stockout_flag  
- ingestion_timestamp  

---

### 3️⃣ Gold Layer (Analytics & KPIs)

#### 📈 Revenue KPIs
- Total Revenue  
- Total Orders  
- Average Order Value  

#### ⚠️ Revenue at Risk
- Revenue lost due to stockouts  

#### 🚚 Supply Chain Metrics
- Delivery delays  
- Fulfillment performance  

---

## 📊 Key Business Insights

This system helps answer:

- How much revenue is lost due to stockouts?  
- Which SKUs or regions are underperforming?  
- How efficient is the supply chain?  
- What are the key drivers of revenue trends?  

---

## ⚙️ Setup Instructions

### 1 Start All Services
docker compose up --build
Services:

Airflow → http://localhost:8080
LocalStack → http://localhost:4566
FastAPI → http://localhost:8000
PostgreSQL

### 2 Airflow Login
Username: admin  
Password: admin  

Trigger DAGs:
Bronze ingestion
Silver transformation
Gold metrics

### 3 Run Spark Jobs (Optional Manual Run)
docker exec -it <container_name> bash

spark-submit src/bronze/bronze_orders_ingest.py
spark-submit src/silver/silver_order_facts.py
spark-submit src/gold/gold_revenue_kpis.py

### 4 Run Streamlit Dashboard
cd streamlit
streamlit run app.py

## What Was Achieved
Built a complete end-to-end data pipeline
Implemented medallion architecture (Bronze → Silver → Gold)
Simulated real-world retail + logistics system
Generated business KPIs and insights
Integrated API + batch data sources
Created analytics-ready datasets for decision making

⚠️ Challenges Faced
Spark + S3 Integration
Issue: S3AFileSystem not found
Fix: Added required Hadoop & AWS dependencies
JAVA_HOME Errors
Spark failed due to missing Java setup
Fixed by configuring environment variables
Docker Dependency Issues
FastAPI container missing uvicorn
Fixed by correcting requirements.txt
Point-in-Time Join Complexity
Needed correct inventory snapshot per order
Solved using window functions
