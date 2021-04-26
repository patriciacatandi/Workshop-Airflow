import numpy as np 
import pandas as pd 
import joblib
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.model_selection import train_test_split


def train_lr(
            input_path,
            save_path, 
            TARGET_COL
            ):
    
    # Lê arquivo com dados já pré-processados 
    df = pd.read_csv(input_path, sep=',').astype('float32').apply(lambda x: x.fillna(x.mean()),axis=0)
    print(df.iloc[0])
    
    # Separa o target dos dados
    X = df.drop([TARGET_COL],axis=1)
    y = df[TARGET_COL]

    # Separa dados em treino e validação
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=4, stratify=y)

    # Instância e treino a regressão logística
    model = LogisticRegression()
    model.fit(X_train,y_train)

    print('Train score:', model.score(X_train, y_train))
    print('Test score:', model.score(X_test, y_test))
    
    # Salva AUC score para definir melhor modelo na predição
    np.save(save_path+'lr_score', roc_auc_score(y, model.predict_proba(X)[:, 1]))

    # Salva pickle do modelo
    joblib.dump(model, save_path+'lr.pkl')


def train_xgboost(
                    input_path,
                    save_path, 
                    TARGET_COL
                    ):

    # Lê arquivo com dados já pré-processados                
    df = pd.read_csv(input_path, sep=',')

    # Separa o target dos dados
    X_train = df.drop([TARGET_COL],axis=1)
    y_train = df[TARGET_COL]

    # Instância o modelo
    alg = XGBClassifier(objective='binary:logistic')

    # Define dicionário de parâmetros a serem testados no GridSearch
    parameters = {'nthread':[4], 
                'objective':['binary:logistic'],
                'learning_rate': [0.05], 
                'max_depth': [2,4,6],
                'min_child_weight': [11],
                'silent': [1],
                'subsample': [0.8],
                'colsample_bytree': [0.7],
                'n_estimators': [50,100,200],
                'missing':[-999],
                'seed': [1337]}

    grid_xgb = GridSearchCV(alg, parameters, n_jobs=3, 
                    cv=StratifiedKFold(n_splits=3, shuffle=True), 
                    scoring='roc_auc',
                    verbose=2, refit=True)

    # Faz o fit do modelo
    grid_xgb.fit(X_train, y_train)
    print(f'Best score: {grid_xgb.best_score_} \n Best params: {grid_xgb.best_params_}')

    # Salva AUC score para definir melhor modelo na predição
    np.save(save_path+'xgboost_score', grid_xgb.best_score_)

    # Salva pickle do modelo
    joblib.dump(grid_xgb, save_path+'xgboost.pkl')