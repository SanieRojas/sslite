import re
import nltk
import pandas as pd
import pandas_gbq
import logging
from datetime import datetime
from GoogleNews import GoogleNews
from google.cloud import bigquery, storage

from nltk.sentiment.vader import SentimentIntensityAnalyzer

import plotly.express as px


def get_tokens(newsfeed):

    """ Clean text."""
    nltk.download(['stopwords', 'vader_lexicon', 'punkt'], quiet=True) 
    nltk.download('maxent_ne_chunker', quiet=True)
    nltk.download('words', quiet=True)
    nltk.download('averaged_perceptron_tagger', quiet=True)
    my_stopwords = nltk.corpus.stopwords.words("english")
    cleaned_text = re.sub(r'[^a-zA-Z\s]', '', newsfeed).lower()

    words = nltk.word_tokenize(cleaned_text)
    tokens = [word for word in words if word not in my_stopwords]

    return tokens

def get_scores(df):
    """ Get scores."""
    scores = []
    analyzer = SentimentIntensityAnalyzer()
    for i in range(len(df)):
        tokens = df["tokens"][i]
        sentiment_score = analyzer.polarity_scores(' '.join(tokens))['compound']
        scores.append(sentiment_score)
    df["score"] = scores
    return df

def extract_entities(txt):
    """ extract main entities."""
    with open(txt, 'r',  encoding='utf-8') as f:
        text = f.read()
    entities = {}
    for sent in nltk.sent_tokenize(text):
        for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
            if hasattr(chunk, 'label'):
                entity = ' '.join(c[0] for c in chunk)
                entities[entity] = entities.get(entity, 0) + 1
    return entities

def setup_engine(period, subject):
    """
    Set up the google engine to retrieve news of a given period.  Performs a search of news for any selected subject theme and, 
    if prompted, saves it into a file
    Documentation of the API wrapper -> https://pypi.org/project/GoogleNews/

    :period: defines how much time to search news for. 
    :type: 7d [quantity of days + "d"] 

    :subject: Subject of interest
    :rtype: String

    :return: filename of the output file
    :rtype: string containing filename  
    """
    # Set up date
    id = str(datetime.now().timestamp())[:10]
    time_stamp = datetime.now().timestamp()
    
    # Quick off instance
    api = GoogleNews()
    api.set_lang("en")
    api.set_encode("utf-8")
    api.set_period(period)
    api.get_news(subject)
    results = api.results(sort=True)

    #Save to dataframe 
    newsfeed = pd.DataFrame(results)
    assert newsfeed.shape[0] >= 1
    newsfeed["log_date"] = time_stamp
    newsfeed["subject"] = subject
    logging.info(f'{newsfeed.dtypes}')
    logging.info("That´s the imported and stamped dataset")
    columns_to_remove = ['desc','site','link','img','media','log_date']
    columns_to_drop = [col for col in columns_to_remove if col in newsfeed.columns]
    newsfeed2 = newsfeed.drop(columns=columns_to_drop, axis=1)
    newsfeed2["tokens"] = newsfeed2["title"].apply(get_tokens)
    newsfeed2 = get_scores(newsfeed2)
    logging.info(f'{newsfeed2.dtypes}')
    logging.info("Types prior to typecasting")
    
    #setting data types
    newsfeed2['datetime'] = pd.to_datetime(newsfeed2['datetime'])

    newsfeed2['title'] = newsfeed2['title'].astype(str)
    newsfeed2['date'] = newsfeed2['date'].astype(str)
    newsfeed2['subject'] = newsfeed2['subject'].astype(str)
    newsfeed2['tokens'] = newsfeed2['tokens'].astype(str)
    newsfeed2['score'] = newsfeed2['score'].astype(float)

    logging.info(f'{newsfeed2.dtypes}')
    logging.info("That´s the final saved dataset")

    file_name = f'/opt/airflow/files/processed/raw_{subject}_{id}.parquet'
    newsfeed2.to_parquet(file_name, index=False)

    return file_name
   

def save_to_gcs(**context):
    '''Set up a function to save the extraction into a GCS Bucket. 

    :context: brings the name of the file result of the function setup_engine 

    :return: Does not have a return specified. 
    '''
    # Carga el DataFrame desde XComs
    file_name = context['task_instance'].xcom_pull(task_ids='extract_and_load_to_df')
    ds = context['ds_nodash']
    # Guarda el DataFrame en un archivo parquet

    # Sube el archivo parquet a Google Cloud Storage
    bucket_name = "subject-screener1"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    future_name = f"raw_data/data_{ds}.parquet"
    blob = bucket.blob(future_name)
    blob.upload_from_filename(file_name)

    return future_name

def load_to_bq(**context):
    ''' Load records retrieved by the setup_engine function into a BigQuery Table'''
    client = bigquery.Client()
    table_id = 'ssdataset.news-by-subject'
    file_name = context['task_instance'].xcom_pull(task_ids='extract_and_load_to_df')
    df = pd.read_parquet(file_name, engine='pyarrow')
    print (df.dtypes)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("datetime", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("subject", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("tokens", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("score", bigquery.enums.SqlTypeNames.FLOAT)], 
            write_disposition="WRITE_APPEND")
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))

def view_scores(**context):
    '''Set up a function to generate an HTML file that displays in a plot the results of the sentiments of news per day. 

    :context: brings the name of the file result of the function setup_engine 

    :return: Does not have a return specified. 
    '''
    file_name = context['task_instance'].xcom_pull(task_ids='extract_and_load_to_df')
    ds = context['ds_nodash']
    df = pd.read_parquet(file_name)
    metadata= df.drop(columns=['title', 'tokens'], axis=1)

    metadata["datetime"] = pd.to_datetime(metadata["datetime"])
    metadata = metadata.sort_values(by="datetime",ascending=True)
    metadata["score"] = metadata["score"].astype(float)

    average_score= metadata['score'].mean()

    daily_indexed = metadata.set_index('datetime')
    mean_by_day = daily_indexed["score"].resample('D').mean()

    print(average_score, mean_by_day)

    daily_score = px.scatter(df, x="datetime", y="score", 
                             title = f"News Sentiment Scores by day. This topic has an average score of: {average_score}, being 1 mostly positive and -1 mostly negative", 
                             color=df['score'] < 0, color_discrete_map={False: 'blue', True: 'red'}, 
                             hover_data=[df['title']] )
    daily_score.write_html(f'/opt/airflow/files/processed/my_plot_{ds}.html')

def generate_summary(**context):
    """Summarize scores by day & load them into BQ table"""
    # [START bigquery_pandas_gbq_read_gbq_legacy]
    client = bigquery.Client()
    project_id = 'subject-screener-402918'
    table_id = 'ssdataset.subject_scorest'
    
    query = """select subject, round(avg(score),2) as avg_score, DATE(datetime) as change_date from `subject-screener-402918.ssdataset.news-by-subject`
        group by subject, change_date
        order by subject, change_date;"""
    
    df2 = pandas_gbq.read_gbq(
        query,
        project_id=project_id,
        dialect="standard",
    )

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("subject", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("avg_score", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("change_date", bigquery.enums.SqlTypeNames.DATE)], 
            write_disposition="WRITE_APPEND")
    
    job = client.load_table_from_dataframe(df2, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
