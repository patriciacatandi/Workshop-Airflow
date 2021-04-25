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
get_prediction = lazy_import.lazy_callable('src.predict.predict.predict_values')

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
    dag_id="WiDS_2021_prediction",                                  # nome da dag
    description="Extrair dados do twitter via airflow",             # Informação sobre a DAG
    default_args=default_args,                                      # Argumentos definidos acima
    schedule_interval=None#timedelta(minutes=60)                    # Intervalo entre cada execução da DAG pode ser cron * 17 * * *
)

# PythonOperator para extrair dados do kaggle
task_extract_data = PythonOperator(
	task_id='extract_data',
	python_callable=get_extract_data,
    op_kwargs={
        'url':"https://storage.googleapis.com/kaggle-competitions-data/kaggle-v2/22359/1852555/compressed/UnlabeledWiDS2021.csv.zip?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1619624035&Signature=SjQBHkAq7%2FYVfqQf6TdILKcZOEgJeCAV133J7RWYYV9tyGtypPjoXxgtZlWje%2FOlEnFJmzSDWpVlz8ESHQCng%2Fewl5OAOmr5vQDP653Pt0OkRQQn%2BPIor5rwmdeED8srhJWMXhMazNNyN%2BayEyBxrgGb3KsJDU7NxQNIMxJV6GbT6GAdf%2FzBtTHMRd3HeLtTpxbN2RC69kPowCcAAiLAranBrHdBns8FUxY0BDags%2B5YultAl2KLvwv8pZ67GbNkHm3aqjPn%2BEbMDGs%2BGuhWw03s7lu%2Bgu3G9iV41QYC0wuhrJ%2BJLxdcNJGVYWp0YedmHHAj7l14FhUAyLndRcBH8A%3D%3D&response-content-disposition=attachment%3B+filename%3DUnlabeledWiDS2021.csv.zip",
        'save_path':workdir+'data/raw/'
        },
	dag=dag   
)

# PythonOperator para fazer o preprocessamento dos dados
task_preprocess_data = PythonOperator(
	task_id='preprocess_data',
	python_callable=get_preprocess_data,
    op_kwargs={
        'input_path':workdir+'data/raw/UnlabeledWiDS2021.csv',
        'save_path':workdir+'data/transform/test_data.csv'
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
task_predict = PythonOperator(
	task_id='prediction',
	python_callable=get_prediction,
    op_kwargs={
        'input_path':workdir+'data/transform/test_data.csv',
        'model_path':workdir+'model/',
		'save_path':workdir+'data/prediction/',
        'TARGET_COL':TARGET_COL
        },
	dag=dag
)

# Definindo fluxo da dag
task_extract_data >> task_preprocess_data >> task_predict