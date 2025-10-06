import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# Command for the first stage (Raw -> Processed)
raw_spark_submit_command = """
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://coms-spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
    /opt/spark/scripts/process_raw_zone.py --table {{ params.table_name }}
"""

# Command for the second stage (Processed -> Curated)
curated_spark_submit_command = """
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://coms-spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
    /opt/spark/scripts/process_curated_zone.py --table {{ params.table_name }}
"""

alerting_spark_submit_command = """
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://coms-spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
    /opt/spark/scripts/handle_payment_alerts.py
"""

with DAG(
    dag_id="coms_end_to_end_etl",
    start_date=pendulum.datetime(2025, 9, 30, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="End-to-end ETL pipeline for the COMS project.",
    tags=["coms", "spark", "etl"],
) as dag:
    # --- STAGE 1: Raw to Processed ---
    raw_tasks = [
        BashOperator(
            task_id=f"process_raw_{table}",
            bash_command=raw_spark_submit_command,
            params={"table_name": table},
        ) for table in ["customers_csv", "employees_csv", "orders_csv", "order_items_csv", "payments_csv"]
    ]

    # --- BARRIER ---
    # This empty task acts as a synchronization point.
    processing_complete = EmptyOperator(task_id="processing_complete")

    # --- STAGE 2: Processed to Curated ---
    curated_tasks = [
        BashOperator(
            task_id=f"create_curated_{table}",
            bash_command=curated_spark_submit_command,
            params={"table_name": table},
        ) for table in ["customer_orders_summary", "order_facts", "daily_sales_aggregates"]
    ]

    # --- BARRIER 2 ---
    curated_complete = EmptyOperator(task_id="curated_complete")
    
    # --- STAGE 3: Advanced Features ---

    run_payment_alerts = BashOperator(
        task_id="run_payment_alerts",
        bash_command=alerting_spark_submit_command,
    )

    # --- DEFINE DEPENDENCIES ---
    # All raw tasks must complete -> then the barrier -> then all curated tasks can start.
    raw_tasks >> processing_complete >> curated_tasks >> curated_complete >> run_payment_alerts