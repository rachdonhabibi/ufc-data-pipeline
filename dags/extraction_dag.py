from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ufc_extraction_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['ufc', 'extraction'],
) as dag:

    extract_event_data = BashOperator(
        task_id='extract_event_data',
        bash_command='python /opt/airflow/src_airflow/extraction/extract_event_data.py',
    )

    extract_event_fights = BashOperator(
        task_id='extract_event_fights',
        bash_command='python /opt/airflow/src_airflow/extraction/extract_event_fights.py',
    )

    extract_fighters_details = BashOperator(
        task_id='extract_fighters_details',
        bash_command='python /opt/airflow/src_airflow/extraction/extract_fighters_details.py',
    )

    extract_strikes = BashOperator(
        task_id='extract_strikes',
        bash_command='python /opt/airflow/src_airflow/extraction/strikes.py',
    )

    extract_totals = BashOperator(
        task_id='extract_totals',
        bash_command='python /opt/airflow/src_airflow/extraction/totals.py',
    )

    trigger_transformation = TriggerDagRunOperator(
        task_id='trigger_transformation_dag',
        trigger_dag_id='ufc_transformation_dag'
    )

    # Set dependencies: event_data -> event_fights -> fighters_details -> [strikes, totals] -> trigger_transformation
    extract_event_data >> extract_event_fights >> extract_fighters_details >> [extract_strikes, extract_totals] >> trigger_transformation
