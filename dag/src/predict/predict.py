import numpy as np 
import pandas as pd 
import joblib
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression

from sklearn.metrics import mean_squared_error, classification_report
from sklearn.model_selection import GridSearchCV, StratifiedKFold
# import utils as ut

from sklearn.model_selection import train_test_split


def predict_values(
            input_path,
            model_path,
            save_path,
            TARGET_COL
            ):

    # Lê dados de teste já pré-processados
    df = pd.read_csv(input_path, sep=',').astype('float')

	# Importa métricas dos modelos
    xgboost_score = np.load(model_path+'xgboost_score.npy')
    lr_score = np.load(model_path+'lr_score.npy')

    # Print das métricas no log do Airflow
    print(f'xgboost_score: {xgboost_score:.4f}      lr_score: {lr_score:.4}')

    # Escolher melhor modelo baseado na métrica
    if xgboost_score > lr_score:
        best_model = joblib.load(model_path+'xgboost.pkl')
    else:
        best_model = joblib.load(model_path+'lr.pkl')
        df = df.apply(lambda x: x.fillna(x.mean()),axis=0)

    # Faz a predição usando o melhor modelo
    pred = best_model.predict(df)

    # Salva predição
    np.save(save_path+'prediction', pred)