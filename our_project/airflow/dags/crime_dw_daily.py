"""
Airflow Daily Orchestration DAG for Chicago Crime Data Warehouse
Schedules daily at 02:00 AM America/Chicago (14:00 PM Bangkok Time)
Integrates dbt using Astronomer Cosmos with Native Lineage & Quality Gates
Derived from Appendix D of Data Warehouse Architecture Design Document V3 (PDF)
"""
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior

chicago_tz = pendulum.timezone("America/Chicago")

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crime_dw_daily_pipeline',
    default_args=default_args,
    description='Daily ELT Refresh for Chicago Crime Data Warehouse',
    schedule='0 2 * * *',  # 02:00 AM Chicago Time (14:00 PM Bangkok Time)
    start_date=pendulum.datetime(2026, 1, 1, tz=chicago_tz),
    catchup=False,
    max_active_runs=1,
    tags=['dw', 'crime', 'dbt', 'cosmos', 'v3'],
) as dag:

    # 1. Ingest Raw Incremental Crime Data from Chicago Data Portal SODA API
    ingest_raw = BashOperator(
        task_id='ingest_raw_crime_data',
        bash_command='python /opt/airflow/ingestion/fetch_daily_crimes.py',
    )

    # 2. Quarantine Gate: Isolate invalid raw records into raw_staging.quarantine_crime_records
    quarantine_crimes = BashOperator(
        task_id='stg_quarantined_crimes',
        bash_command=(
            'dbt run --project-dir /opt/airflow/dbt '
            '--profiles-dir /opt/airflow/dbt '
            '--target prod --select stg_quarantined_crimes'
        ),
    )

    # 3. Cosmos DbtTaskGroup: Renders dbt staging, core models, tests, and marts
    dbt_tasks = DbtTaskGroup(
        group_id='dbt_transforms_and_tests',
        project_config=ProjectConfig('/opt/airflow/dbt'),
        profile_config=ProfileConfig(
            profile_name='crime_dw',
            target_name='prod',
            profiles_yml_filepath='/opt/airflow/dbt/profiles.yml',
        ),
        render_config=RenderConfig(
            exclude=['stg_quarantined_crimes'],  # Executed in Task 2
            test_behavior=TestBehavior.AFTER_EACH,  # Block downstream marts on core test failure
        ),
    )

    # 4. Trigger Metabase Cache Refresh
    refresh_metabase = BashOperator(
        task_id='refresh_metabase_cache',
        bash_command='python /opt/airflow/scripts/refresh_metabase_cache.py',
    )

    # Pipeline Execution Order (Matches Table 27 in V3 PDF)
    ingest_raw >> quarantine_crimes >> dbt_tasks >> refresh_metabase
