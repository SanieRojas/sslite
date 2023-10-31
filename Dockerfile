FROM apache/airflow:2.7.2-python3.8
WORKDIR TP_PDA
COPY requirements.txt .
RUN pip install -r requirements.txt 
