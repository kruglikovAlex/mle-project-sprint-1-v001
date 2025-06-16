# dags/clean_cost_estimate.py

import pendulum
import pandas as pd
from airflow.decorators import dag, task
from steps.message import send_telegram_success_message, send_telegram_failure_message

def remove_duplicates(data):
    # проверка на дубликаты
    feature_cols = data.columns.drop({'id','build_type_floors'}).tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    
    return data 

def fill_missing_values(data):
    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index

    for col in cols_with_nans:

        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]

        data[col] = data[col].fillna(fill_value)

    return data

def outliers(data):
    num_cols = data.select_dtypes(['float']).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        Q1 = data[col].min() 
        Q3 = data[col].max() 
        IQR = Q3 - Q1 
        margin =threshold * IQR 
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    data = data[~outliers].reset_index(drop=True)

    return data

def fill_zero_values(data):
    # проверка на 0 в колонках с площадью
    data['total_area'] = data['total_area'].astype(float)
    data['living_area'] = data['living_area'].astype(float)
    data['kitchen_area'] = data['kitchen_area'].astype(float)

    # 1. проверяем на 0.0 в поле 'total_area'
    zero_area_val = data[data['total_area'].isin([0.0])]
    if zero_area_val.shape[0] > 0:
        data.loc[~data['living_area'].isin([0.0]) & ~data['kitchen_area'].isin([0.0]), 'total_area'] = data['living_area'] + data['kitchen_area']

    # 2. проверяем на 0.0 в полях 'living_area' и 'kitchen_area' одновременно",
    zero_area_val = data[data['living_area'].isin([0.0]) & data['kitchen_area'].isin([0.0]) & ~data['total_area'].isin([0.0])]
    # распределим площади пропорционально учитывая количество комнат, т.е.",
    # если комната одна, то жилая к нежилой 70%/30%",
    # если комнат больше одной, то жилая к кухне - 50%/30%",
    if zero_area_val.shape[0] > 0:
        data.loc[data['rooms'] == 1 & data['living_area'].isin([0.0]) & data['kitchen_area'].isin([0.0]) & ~data['total_area'].isin([0.0]), 'living_area'] = data['total_area']*0.7
        data.loc[data['rooms'] == 1 & data['living_area'].isin([0.0]) & data['kitchen_area'].isin([0.0]) & ~data['total_area'].isin([0.0]), 'kitchen_area'] = data['total_area']*0.3
        data.loc[data['rooms'] > 1 & data['living_area'].isin([0.0]) & data['kitchen_area'].isin([0.0]) & ~data['total_area'].isin([0.0]), 'living_area'] = data['total_area']*0.5
        data.loc[data['rooms'] > 1 & data['living_area'].isin([0.0]) & data['kitchen_area'].isin([0.0]) & ~data['total_area'].isin([0.0]), 'kitchen_area'] = data['total_area']*0.3
    # 3. проверяем на 0.0 в 'living_area' или 'kitchen_area'
    zero_area_val = data[data['living_area'].isin([0.0]) | data['kitchen_area'].isin([0.0])]
    if zero_area_val.shape[0] > 0:
        data.loc[data['living_area'].isin([0.0]) & ~data['kitchen_area'].isin([0.0]) & ~data['total_area'].isin([0.0]), 'living_area'] = data['total_area'] - data['kitchen_area']
        data.loc[~data['living_area'].isin([0.0]) & data['kitchen_area'].isin([0.0]) & ~data['total_area'].isin([0.0]), 'kitchen_area'] = data['total_area'] - data['living_area']
    # 4. проверка на 0 в колонках с ценой и высотой этажа
    data['price'] = data['price'].astype(int)
    data['ceiling_height'] = data['ceiling_height'].astype(float)
    cols_with_nans = data[['price','ceiling_height']].isin([0]).sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index
    for col in cols_with_nans:
        fill_value = data[col].mean()

        data[col] = data[col].fillna(fill_value)

    return data

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2025, 6, 16, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def clean_cost_estimate_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        from sqlalchemy import Table, Column, DateTime, Float, Integer, BIGINT, Index, MetaData, String, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        clean_cost_estimate = Table(
            'clean_cost_estimate', 
            metadata,
            Column('id', BIGINT, primary_key=True, autoincrement=True), 
            Column('floor', BIGINT), 
            Column('is_apartment', Integer), 
            Column('kitchen_area', Float),
            Column('living_area', Float), 
            Column('rooms', BIGINT), 
            Column('studio', Integer),
            Column('total_area', Float),
            Column('price', BIGINT), 
            Column('building_id', BIGINT),
            Column('build_year', BIGINT),
            Column('building_type_int', BIGINT),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', BIGINT),
            Column('floors_total', BIGINT),
            Column('has_elevator', Integer),
            Column('build_type_floors', String),
            UniqueConstraint('id', name='unique_clean_flats_constraint')
        )
        if not inspect(db_engine).has_table(clean_cost_estimate.name):
            metadata.create_all(db_engine)

    @task()
    def extract():
        hook = PostgresHook('source_db')
        conn = hook.get_conn()
        sql = f"""
        SELECT * FROM public.cost_estimate
        """
        data = pd.read_sql(sql, conn) #.drop(columns=['id'])
        conn.close()
        return data
    
    @task()
    def transform(data: pd.DataFrame):
        data = remove_duplicates(data)
        data = fill_missing_values(data)
        data = outliers(data)
        data = fill_zero_values(data)

        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table= 'clean_cost_estimate',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_cost_estimate_dataset()