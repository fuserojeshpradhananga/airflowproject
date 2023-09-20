from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
import requests
import csv
import os



default_args = {
    'owner': 'rojesh',
    'start_date': datetime(2023, 9, 18),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'my_project_dag',
    default_args=default_args,
    description='Your DAG description',
    schedule_interval=timedelta(days=1),
    start_date= default_args['start_date'],
    catchup=False, 
)

check_api_task = SimpleHttpOperator(
    task_id='check_api_availability',
    http_conn_id='http_local', 
    method='GET',
    endpoint='/v2/top-headlines/sources?apiKey=7dc5539bb1d94904b8c5a0db7d44e889',
    dag=dag,
)


output_dir = '/csv/'

def extract_data_and_convert_to_csv():
    api_url = Variable.get("api")

    try:
        response = requests.get(api_url)

        if response.status_code == 200:
            data = response.json()
            
            
            sources = data.get('sources', [])
            
            csv_file_path = os.path.join(output_dir, 'api_data.csv')

            with open(csv_file_path, 'w', newline='') as csv_file:
                fieldnames = ['id', 'name', 'description', 'url', 'category', 'language', 'country']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                for source in sources:
                    writer.writerow({
                        'id': source.get('id', ''),
                        'name': source.get('name', ''),
                        'description': source.get('description', ''),
                        'url': source.get('url', ''),
                        'category': source.get('category', ''),
                        'language': source.get('language', ''),
                        'country': source.get('country', ''),
                    })

            return csv_file_path
        else:
            print(f"API returned a non-200 status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

extract_data_task = PythonOperator(
    task_id='extract_data_and_convert_to_csv',
    python_callable=extract_data_and_convert_to_csv,
    dag=dag,
)



csv_file_path = ('/csv/api_data.csv')

#defining the PostgreSQL connection ID
postgres_conn_id = 'postgres_local'


file_sensing_temp = FileSensor(
        task_id="file_sensing_in_tmp_folder",
        filepath="../../../../tmp/csv/",
        poke_interval= 20,
        mode="poke"            
    )


move_file_task = BashOperator(
    task_id='move_file_to_tmp',
    bash_command='mv /csv/api_data.csv /tmp/csv/',
    dag = dag
)





copy_csv_to_postgres_task = PostgresOperator(
    task_id='copy_csv_to_postgres',
    postgres_conn_id=postgres_conn_id,
    sql=f"COPY airflow_input2 FROM '/tmp/csv/api_data.csv' WITH CSV HEADER",
    dag=dag
)




spark_submit = BashOperator(
        task_id='spark_submit_task',
        bash_command="spark-submit /spark.py",
    )


read_table = PostgresOperator(
        sql = "select * from answer1",
        task_id = "read_table_task",
        postgres_conn_id = postgres_conn_id,
        autocommit=True,
    )

def process_postgres_result(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='read_table_task')
        # Process the result data here
        for row in result:
            print(row)
       
    

process_data_task = PythonOperator(
        task_id='process_postgres_result',
        python_callable=process_postgres_result,
        provide_context=True
    )
 

check_api_task   >> extract_data_task >> move_file_task >> file_sensing_temp >>  copy_csv_to_postgres_task >> spark_submit >> read_table >> process_data_task
