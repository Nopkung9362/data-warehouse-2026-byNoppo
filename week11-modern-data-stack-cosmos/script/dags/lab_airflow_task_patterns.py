from airflow import DAG
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import os


def notify_failure_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    logical_date = context.get('data_interval_start')
    exception = context.get('exception')

    print(f"[ALERT FAILURE] DAG: '{dag_id}' | Task: '{task_id}'")
    print(f"Execution Logical Date: {logical_date}")
    print(f"Error Cause: {exception}")


def check_data_freshness_branch(**kwargs):
    file_exists = os.path.exists('/home/raw_data/olist_orders_dataset.csv')
    if file_exists:
        # คืนค่าเป็น list ของทุก task ภายใน ingestion_group เพื่อให้ทำงานขนานกันทั้งหมด
        return [
            'ingestion_group.ingest_orders',
            'ingestion_group.ingest_items',
            'ingestion_group.ingest_customers'
        ]
    else:
        return 'skip_pipeline_notice'


default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notify_failure_callback,
}

with DAG(
    dag_id='lab_airflow_task_patterns',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2017, 1, 1),
    catchup=False,
    tags=['production', 'task_patterns', 'olist'],
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # Pattern 1: Branching Pattern
    branch_check = BranchPythonOperator(
        task_id='branch_check_data_freshness',
        python_callable=check_data_freshness_branch,
    )

    skip_pipeline_notice = EmptyOperator(
        task_id='skip_pipeline_notice'
    )

    # Pattern 2: Parallel Execution Pattern (Fan-out / Fan-in) ใน TaskGroup
    with TaskGroup(group_id='ingestion_group') as ingestion_group:
        ingest_orders = SQLExecuteQueryOperator(
            task_id='ingest_orders',
            conn_id='postgres_dwh',
            sql="SELECT 1; -- Ingest Orders Process"
        )

        ingest_items = SQLExecuteQueryOperator(
            task_id='ingest_items',
            conn_id='postgres_dwh',
            sql="SELECT 1; -- Ingest Items Process"
        )

        ingest_customers = SQLExecuteQueryOperator(
            task_id='ingest_customers',
            conn_id='postgres_dwh',
            sql="SELECT 1; -- Ingest Customers Process"
        )

    # Pattern 3: Sequential Flow
    audit_raw_data = SQLExecuteQueryOperator(
        task_id='audit_raw_data_quality',
        conn_id='postgres_dwh',
        sql="SELECT COUNT(*) FROM olist_raw.olist_orders;"
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule='none_failed_min_one_success'
    )

    start_pipeline >> branch_check
    branch_check >> skip_pipeline_notice >> end_pipeline
    branch_check >> ingestion_group >> audit_raw_data >> end_pipeline
