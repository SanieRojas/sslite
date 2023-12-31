''' DAG to process extraction daily'''
from datetime import datetime
from functions import setup_engine, save_to_gcs, load_to_bq, view_scores, generate_summary 
from variables import my_variables
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

###################################################################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 29)
}

dag = DAG('first_dag_SR', 
          description='Extract data from API, load into pandas, save to GCS, and view scores by day',
          schedule_interval='0 12 * * *',
          default_args=default_args,
          catchup=False)

task1 = PythonOperator(task_id='extract_and_load_to_df', 
                       python_callable=setup_engine,
                       op_args = my_variables, 
                       provide_context=True,
                       dag=dag)

task2 = PythonOperator(task_id='save_to_gcs', 
                       python_callable=save_to_gcs, 
                       provide_context=True,
                       dag=dag)

task3 = PythonOperator(task_id='load_to_bq', 
                       python_callable=load_to_bq, 
                       provide_context=True,
                       dag=dag)

task4 = PythonOperator(task_id='generate_summary', 
                       python_callable=generate_summary, 
                       provide_context=True,
                       dag=dag)

task5 = PythonOperator(task_id='view_scores', 
                       python_callable=view_scores, 
                       dag=dag)

task1 >> (task2, task3, task5)
task3 >> task4