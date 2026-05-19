from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_revenue_intelligence_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ── Bronze ──────────────────────────────────────────────────────────
    bronze_orders_task = BashOperator(
        task_id="bronze_orders_ingest",
        bash_command="python /opt/airflow/src/spark_jobs/bronze/bronze_orders_ingest.py",
    )

    bronze_inventory_task = BashOperator(
        task_id="bronze_inventory_ingest",
        bash_command="python /opt/airflow/src/spark_jobs/bronze/bronze_inventory_ingest.py",
    )

    bronze_logistics_task = BashOperator(
        task_id="bronze_logistics_ingest",
        bash_command="python /opt/airflow/src/spark_jobs/bronze/bronze_logistics_ingest.py",
    )

    # ── Silver ──────────────────────────────────────────────────────────
    silver_order_facts_task = BashOperator(
        task_id="silver_order_facts",
        bash_command="python /opt/airflow/src/spark_jobs/silver/silver_order_facts.py",
    )

    # ── Gold ────────────────────────────────────────────────────────────
    gold_revenue_kpis_task = BashOperator(
        task_id="gold_revenue_kpis",
        bash_command="python /opt/airflow/src/spark_jobs/gold/gold_revenue_kpis.py",
    )

    gold_revenue_at_risk_task = BashOperator(
        task_id="gold_revenue_at_risk",
        bash_command="python /opt/airflow/src/spark_jobs/gold/gold_revenue_at_risk.py",
    )

    gold_supply_chain_task = BashOperator(
        task_id="gold_supply_chain_metrics",
        bash_command="python /opt/airflow/src/spark_jobs/gold/gold_supply_chain_metrics.py",
    )

    # ── Pipeline order ──────────────────────────────────────────────────
    # Logistics reads order IDs from S3, so orders must land first.
    # Inventory is independent and can run in parallel with orders.
    bronze_orders_task >> bronze_logistics_task
    [bronze_orders_task, bronze_inventory_task, bronze_logistics_task] \
        >> silver_order_facts_task \
        >> [gold_revenue_kpis_task, gold_revenue_at_risk_task, gold_supply_chain_task]
