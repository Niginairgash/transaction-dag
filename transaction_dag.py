from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl_script_transactions import extract_data, transform_data, load_data_to_db
import logging

#logger Airflow
log = logging.getLogger('airflow.task')



#default parameters for DAG
default_args = {
    'owner' : 'airflow',
    'start_date' :  days_ago(1),
    'retries' : 1,
}


# create DAG
with DAG(
    'etl_pipline_transaction', 
    default_args=default_args,
     schedule_interval='@daily' ) as dag:
    
    #define tasks

    def extract_task():
        log.info("Extracting data")
        return extract_data(r'/path/to/your/transactions.csv')
    
    extract = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_task
    )

    def transform_task():
        log.info("Data transforming task in progress")
        data = extract_task()
        return transform_data(data)
    
    transform = PythonOperator(
        task_id = 'transform_data',
        python_callable=transform_task
    )


    def load_task():
        log.info("Data loading task in progress")
        data = transform_task()
        load_data_to_db(data, 'postgresql://username:password@localhost:5432/etl_project')

    load = PythonOperator(
        task_id = 'load_data',
        python_callable = load_task 
    )

    extract >> transform >> load
