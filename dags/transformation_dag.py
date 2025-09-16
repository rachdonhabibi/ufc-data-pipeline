from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
#
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ufc_transformation_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['ufc', 'transformation'],
) as dag:

    transform_events = BashOperator(
        task_id='transform_events',
        bash_command='python /opt/airflow/src_airflow/transformation/transform_events.py',
    )

    transform_fight_details = BashOperator(
        task_id='transform_fight_details',
        bash_command='python /opt/airflow/src_airflow/transformation/transform_fight_details.py',
    )

    transform_fighters = BashOperator(
        task_id='transform_fighters',
        bash_command='python /opt/airflow/src_airflow/transformation/transform_fighters.py',
    )

    transform_fights = BashOperator(
        task_id='transform_fights',
        bash_command='python /opt/airflow/src_airflow/transformation/transform_fights.py',
    )

    trigger_loading = TriggerDagRunOperator(
        task_id='trigger_loading_dag',
        trigger_dag_id='ufc_loading_dag'
    )

    transform_events >> [transform_fight_details, transform_fighters, transform_fights] >> trigger_loading