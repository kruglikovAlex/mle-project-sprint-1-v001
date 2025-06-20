# scripts/fit.py

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder
from catboost import CatBoostRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import yaml
import os
import joblib

# — вспомогательные функции:
# --- расстояние от квартиры до центра Москвы
def prepare_data(data):
    import geopy.distance

    # удалим колонки не нужные для обучения
    data.drop(columns=['building_id'], inplace=True) 

    # столбцы с геокординатами в чистом виде нам не нужны
    # в место координат в качестве фичи буду использовать расстояния.
    # будем использовать удаленность от центра Москвы
    # добавим служебные колонки с координататми центра Москвы
    data["latitude_2"] = 55.7522
    data["longitude_2"] = 37.6156
    
    # вычислим дистанцию
    data['dist_origin_dest'] = list(map(geopy.distance.geodesic, data.loc[:, ['latitude', 'longitude']].values, data.loc[:, ["latitude_2", "longitude_2"]].values))
    # приведем ее к типу float
    data['dist_origin_dest'] = data['dist_origin_dest'].map(lambda x: x.km)

    return data

def get_geo_station_metro():
    import requests

    url = "https://api.hh.ru/metro/1"
    response = requests.get(url)

    # Проверяем статус
    if response.status_code == 200:
        data_resp = response.json()
        
        stations = []
        
        # Итерируем по линиям метро
        for line in data_resp['lines']:
            for station in line['stations']:
                stations.append({
                    'id': station['id'],
                    'name': station['name'],
                    'lat': station['lat'],
                    'lng': station['lng'],
                    'line_id': line['id'],
                    'line_name': line['name'],
                    'line_color': line['hex_color'],
                })
        df_station = pd.DataFrame(stations)

        df_station.to_csv("./data/moscow_metro_stations.csv", index=False, encoding='utf-8')
        
        # Оставляем нужные столбцы и переименовываем
        moscow_stations = df_station.drop(columns=['id', 'line_id', 'line_name', 'line_color'])
        moscow_stations.columns = ['station_name', 'lat', 'lon']

        return moscow_stations
    
# --- расстояние от ближайшей станции метро
def calculate_distance_to_nearest_metro(data, moscow_stations):
    from sklearn.neighbors import BallTree
    import numpy as np

    metro_coords = np.radians([[s.lat, s.lon] for s in moscow_stations.itertuples()])

    # Строим дерево
    tree = BallTree(metro_coords, metric='haversine')

    # Координаты квартир в радианах
    flat_coords = np.radians(data[["latitude", "longitude"]].values)
    flat_coords[:5]

    # Ищем ближайшую станцию
    distances_rad, _ = tree.query(flat_coords, k=1)
    # Переводим в метры (радиус Земли ~6371 км)
    data["distance_to_metro_fast"] = distances_rad[:, 0] * 6371000

    # удалим служебные колонки и уже не нужные колонки с координататми
    data.drop(columns=['latitude', 'longitude', 'latitude_2', 'longitude_2'], inplace=True) 

    return data

# — главная функция
# обучение модели
def fit_model():
    
    # Прочитаем файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd) 

    # загружаем результат предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')

    # подготовка банных для обучения
    data = prepare_data(data)
    moscow_stations = get_geo_station_metro()
    data = calculate_distance_to_nearest_metro(data, moscow_stations)

    # сохранение полученных/модифицированных данных на шаге
    os.makedirs('data', exist_ok=True)
    data.to_csv('data/update_data.csv', index=None)

    # обучение модели
    y = data['price']
    data.drop(columns=['price'], inplace=True)
    X = data

    cat_features = data.select_dtypes(include='object')
    potential_binary_features = data.nunique() == 2
    potential_binary_features.studio = True

    binary_cat_features = data[potential_binary_features[potential_binary_features].index]
    other_cat_features = cat_features
    num_features = data.select_dtypes(['float','int'])
    # уберем из числовых бинарные признаки
    num_features.drop(columns=potential_binary_features[potential_binary_features].index, inplace=True) 

    # 1) - библиотека catboost модель CatBoostRegressor
    preprocessor = ColumnTransformer(
        [
            ('binary', 'passthrough', binary_cat_features.columns.tolist()),
            ('cat', 'passthrough', other_cat_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    #cat_features_all = ['build_type_floors', 'dist_origin_dest', 'distance_to_metro_fast', 'is_apartment_1', 'studio_0', 'has_elevator_1']
    cat_feature_indices = list(range(len(binary_cat_features.columns) + len(other_cat_features.columns)))
    model = CatBoostRegressor(iterations=params['iterations'],
                              learning_rate=params['learning_rate'],
                              depth=params['depth'],
                              cat_features=cat_feature_indices,
                              verbose=params['verbose'],
                              early_stopping_rounds=params['early_stopping_rounds']
                              )

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )

    pipeline.fit(data, y) 

    # сохранение результата шага
    os.makedirs('models', exist_ok=True) # создание директории, если её ещё нет
    with open('models/fitted_model_CBR.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd) 
    
    # 2) - библиотека sklearn.linear_model модель LinearRegression
    preprocessor_lr = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cat_features.columns.tolist()),
            ('cat', CatBoostEncoder(return_df=False), other_cat_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    pipeline_lr = Pipeline([
        ('preprocessor', preprocessor_lr),  # лучше использовать OHE для линейных моделей
        ('model', LinearRegression())
    ])

    pipeline_lr.fit(data, y) 

    # сохранение результата шага
    os.makedirs('models', exist_ok=True) # создание директории, если её ещё нет
    with open('models/fitted_model_LR.pkl', 'wb') as fd:
        joblib.dump(pipeline_lr, fd) 
    
    # 3) - библиотека sklearn.ensemble модель RandomForestRegressor
    preprocessor_rfr = ColumnTransformer(
        transformers=[
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cat_features.columns.tolist()),
            ('cat', OrdinalEncoder(handle_unknown=params['handle_unknown'], unknown_value=params['unknown_value']), other_cat_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ]
    )

    model_rfr = RandomForestRegressor(n_estimators=params['n_estimators'], random_state=params['random_state'])

    pipeline_rfr = Pipeline([
        ('preprocessor', preprocessor_rfr),  
        ('model', model_rfr)
    ])

    pipeline_rfr.fit(data, y) 

    # сохранение результата шага
    os.makedirs('models', exist_ok=True) # создание директории, если её ещё нет
    with open('models/fitted_model_RFR.pkl', 'wb') as fd:
        joblib.dump(pipeline_rfr, fd)

if __name__ == '__main__':
	fit_model()