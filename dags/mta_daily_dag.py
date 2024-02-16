from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from scripts.fetch_wifi_data import fetch_wifi_data
from scripts.fetch_subway_hourly_data import fetch_subway_hourly_data
from scripts.aggregate_data import aggregate
from airflow.models import Variable

start_date = datetime(2022, 1, 25, 12, 0)
owner = Variable.get("user")
default_args = {
    'owner': owner,
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

wifi_table_name, subway_table_name, agg_table_name = 'wifi_locations', 'subway_hourly', 'total_subway_ridership'

with DAG(dag_id='mta_daily_dag',
         max_active_runs=8,
         default_args=default_args,
         schedule_interval='@daily',
         catchup=True,
         ) as dag:
    fetch_wifi_location_data = PythonOperator(
        task_id='fetch_wifi_data',
        python_callable=fetch_wifi_data,
        op_kwargs={'wifi': wifi_table_name},
        retries=1,
        retry_delay=timedelta(seconds=15))
    fetch_subway_hourly_data = PythonOperator(
        task_id='fetch_subway_hourly_data',
        python_callable=fetch_subway_hourly_data,
        op_kwargs={'subway': subway_table_name},
        retries=1,
        retry_delay=timedelta(seconds=15))
    aggregate_results = PythonOperator(
        task_id='test_agg',
        python_callable=aggregate,
        op_kwargs={'wifi': wifi_table_name, 'subway': subway_table_name, 'aggregation': agg_table_name},
        retries=1,
        retry_delay=timedelta(seconds=15))
    fetch_wifi_location_data >> fetch_subway_hourly_data >> aggregate_results