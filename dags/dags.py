import pandas as pd
import os 
import re

import nltk
import pandas as pd
import plotly.express as px
from datetime import datetime

from GoogleNews import GoogleNews
from nltk import ne_chunk, pos_tag
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize

nltk.download(['stopwords', 'vader_lexicon', 'punkt'], quiet=True) 
nltk.download('maxent_ne_chunker', quiet=True)
nltk.download('words', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from google.cloud import bigquery, storage

from functions import get_tokens, get_scores, extract_entities, setup_engine, save_to_gcs, view_scores 

###################################################################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 29)
}

my_variables = {
    '10d': '10d',
    'artificial general intelligence': "artificial general intelligence"

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

task3 = PythonOperator(task_id='view_scores', 
                       python_callable=view_scores, 
                       dag=dag)

task1 >> task2
task1 >> task3