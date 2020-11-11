import datetime as dt
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

local_tz = pendulum.timezone("America/Sao_Paulo")
args = {
    'owner': 'me',
    'start_date': dt.datetime(2020, 11, 10,  tzinfo=local_tz),
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG('xcom_example', default_args=args, catchup=False, schedule_interval=None) as dag:
    def current_timestamp(**kwargs):
        current_timestamp  = dt.datetime.strftime(dt.datetime.now(),"%Y-%m-%d %H:%M")
        kwargs['ti'].xcom_push(key='current_timestamp', value=current_timestamp)

    xcom_push = PythonOperator(task_id='xcom_push',    
                               python_callable=current_timestamp,
                               provide_context=True)

    echo_xcom = BashOperator(task_id='echo_xcom',
                             bash_command='echo {{ task_instance.xcom_pull(key="current_timestamp", task_ids="xcom_push")}}')
    
    xcom_push >> echo_xcom