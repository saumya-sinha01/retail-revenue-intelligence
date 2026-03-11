from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_revenue_intelligence_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    bronze_logistics_task = BashOperator(
        task_id="bronze_logistics_ingest",
        bash_command="python /opt/airflow/src/spark_jobs/bronze/bronze_logistics_ingest.py",
    )

    bronze_orders_task = BashOperator(
    task_id="bronze_orders_ingest",
    bash_command="python /opt/airflow/src/spark_jobs/bronze/bronze_orders_ingest.py",
    )

    bronze_inventory_task = BashOperator(
    task_id="bronze_inventory_ingest",
    bash_command="python /opt/airflow/src/spark_jobs/bronze/bronze_inventory_ingest.py",
    )

    bronze_orders_task >> bronze_inventory_task >> bronze_logistics_task