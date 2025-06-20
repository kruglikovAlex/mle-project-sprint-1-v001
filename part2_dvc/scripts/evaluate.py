import pandas as pd
from sklearn.model_selection import KFold, cross_validate
import yaml
import os
import numpy as np
import json
import joblib

def evaluate_model():
    
    # Прочитаем файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd) 

    # загружаем результат шага 1: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')

    # загружаем результат шага 2: загрузка модели
    with open('models/fitted_model_CBR.pkl', 'rb') as fd:
        pipeline = joblib.load(fd)
    
    with open('models/fitted_model_LR.pkl.pkl', 'rb') as fd:
        pipeline_lr = joblib.load(fd)

    with open('models/fitted_model_RFR.pkl', 'rb') as fd:
        pipeline_rfr = joblib.load(fd)

    # Проверка качества на кросс-валидации
    cv_strategy = KFold(n_splits=params['n_splits'], shuffle=params['shuffle'], random_state=params['random_state'])
    
    for name, pipl in [('CBR', pipeline), ('LR', pipeline_lr), ('RFR', pipeline_rfr)]:
        cv_res = cross_validate(
            pipl,
            data,
            data[params['target_col']],
            cv=cv_strategy,
            n_jobs=params['n_jobs'],
            scoring=params['metrics'],
            return_train_score=True
        )

        # Усредняем и округляем
        for key, value in cv_res.items():
            if isinstance(value, (list, np.ndarray)):
                cv_res[key] = round(np.mean(value), 3)
            else:
                cv_res[key] = round(value, 3)

        # Сохраняем
        os.makedirs('cv_results', exist_ok=True)
        with open(f'cv_results/cv_res_{name}.json', 'a') as fd:
            json.dump(cv_res, fd, indent=2)

if __name__ == '__main__':
	evaluate_model()