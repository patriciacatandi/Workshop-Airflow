# Importar libs
from airflow import DAG
from airflow.operators.bash_operator import BashOperator            # para tasks com bash
from airflow.operators.python_operator import PythonOperator        # para executar pyyhon
from datetime import datetime, timedelta
import pandas as pd
import os
import lazy_import

get_extract_data = lazy_import.lazy_callable('src.extract_data.extract_data.run_extract_data_kaggle')
get_preprocess_data = lazy_import.lazy_callable('src.preprocessing.preprocessing.run_preprocessing')
get_separate_data = lazy_import.lazy_callable('src.train.train.run_separa_data')
get_lr_model = lazy_import.lazy_callable('src.train.train.train_lr')
get_xgboost_model = lazy_import.lazy_callable('src.train.train.train_xgboost')

# Constantes
workdir = '/home/patricia/Documentos/Workshop-Airflow/'
TARGET_COL = 'diabetes_mellitus'

# Argumentos default para criação da DAG
default_args = {
	    'owner': 'Desafio_1-Twitter',                       		# é o 'dono/proprietario' da DAG, informar um nome fácil para identificação
	    'depends_on_past': False,                                   # se tem dependências com DAGs anteriores, deixar como False para manter a DAG como autônoma
	    'start_date': datetime(2021, 4, 10, 0, 00),                  # Data de início da DAG
	    'email': ['email1@email1.com.br', 'email2@email2.com.br'],  # Email que irá receber informações sobre a DAG
	    'email_on_failure': False,                                  # Se deseja ser notificado a cada falha que ocorrer na DAG
	    'email_on_retry': False,                                    # Se der alguma falha, tentar fazer nova notificação 
	    'retries': 1,                                               # Em caso de falha quantas tentativas serão notificadas
	    'retry_delay': timedelta(minutes=1)                         # Em caso de falha qual o tempo de tentativa entre uma notificação e outra 
	}

# Define informações da DAG
dag = DAG(
    dag_id="WiDS_2021_train",                                       # nome da dag
    description="Extrair dados do twitter via airflow",             # Informação sobre a DAG
    default_args=default_args,                                      # Argumentos definidos acima
    schedule_interval=None#timedelta(minutes=60)                    # Intervalo entre cada execução da DAG pode ser cron * 17 * * *
)

# PythonOperator para extrair dados do kaggle
task_extract_data = PythonOperator(
	task_id='extract_data',
	python_callable=get_extract_data,
    op_kwargs={
        'url':"https://storage.googleapis.com/kaggle-competitions-data/kaggle-v2/22359/1852555/compressed/TrainingWiDS2021.csv.zip?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1618328198&Signature=JBLvndu5F2z6rSRa7MPUFD3IIWkY%2BUb1edGgcN5PITON2TpElJpQpIkurmmGQ2Q6UGYWeJ8I5E7PZ%2FhaKwwe0Z%2FwUdinG5%2F50NuSbcWNFeFuAbCQwEz9vtCVITBsT5TIhxlw459GMfiIUYqPHEdkLA5dcG5nfjitNp7UNCaS51QCVmdKzjeszPPPcsb%2BZZEVOX%2BScHgJZJLCyygylEUMbdkKAqlCEke0SZ%2FbhtsECOpGsiFRbPmMf9uIObJue%2B7OkFSgB3HXCBKOMHp0fFRCiaSmsryyfGHLxWq1joa%2F914R4REZDwtZa9hNLg%2B4W8mNEW04MVrRftFLzEKBAUIsOA%3D%3D&response-content-disposition=attachment%3B+filename%3DTrainingWiDS2021.csv.zip",
        'save_path':workdir+'data/raw/'
        },
	dag=dag
)

# PythonOperator para fazer o preprocessamento dos dados
task_preprocess_data = PythonOperator(
	task_id='preprocess_data',
	python_callable=get_preprocess_data,
    op_kwargs={
        'input_path':workdir+'data/raw/TrainingWiDS2021.csv',
        'save_path':workdir+'data/transform/train_data.csv'
        },
	dag=dag
)

# # PythonOperator para fazer o preprocessamento dos dados
# task_separate_data = PythonOperator(
# 	task_id='separate_data',
# 	python_callable=get_separate_data,
#     op_kwargs={
#         'input_path':workdir+'data/transform/transform_data.csv',
#         'save_path':workdir+'data/train_test/'
#         },
# 	dag=dag
# )

# PythonOperator para fazer o treinamento dos dados
task_lr = PythonOperator(
	task_id='train_lr',
	python_callable=get_lr_model,
    op_kwargs={
        'input_path':workdir+'data/transform/train_data.csv',
        'save_path':workdir+'model/',
        'TARGET_COL':TARGET_COL
        },
	dag=dag
)

# PythonOperator para fazer o treinamento dos dados
task_xgboost = PythonOperator(
	task_id='train_xgboost',
	python_callable=get_xgboost_model,
    op_kwargs={
        'input_path':workdir+'data/transform/train_data.csv',
        'save_path':workdir+'model/',
        'TARGET_COL':TARGET_COL
        },
	dag=dag
)

# Definindo fluxo da dag
task_extract_data >> task_preprocess_data >> [task_lr, task_xgboost]