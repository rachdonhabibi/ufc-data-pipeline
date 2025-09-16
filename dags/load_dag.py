from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
#
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ufc_loading_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['ufc', 'loading'],
) as dag:

    load_geo_dim = BashOperator(
        task_id='load_geo_dim',
        bash_command='python /opt/airflow/src_airflow/Loading/load_geo_dim.py',
    )

    load_ufc_event = BashOperator(
        task_id='load_ufc_event',
        bash_command='python /opt/airflow/src_airflow/Loading/load_ufc_event.py',
    )

    load_fighters = BashOperator(
        task_id='load_fighters',
        bash_command='python /opt/airflow/src_airflow/Loading/load_fighters.py',
    )

    load_fights = BashOperator(
        task_id='load_fights',
        bash_command='python /opt/airflow/src_airflow/Loading/load_fights.py',
    )

    load_fights_details = BashOperator(
        task_id='load_fights_details',
        bash_command='python /opt/airflow/src_airflow/Loading/load_fights_details.py',
    )

    # Set dependencies as needed
    load_geo_dim >> load_ufc_event >> load_fighters >> load_fights >> load_fights_details