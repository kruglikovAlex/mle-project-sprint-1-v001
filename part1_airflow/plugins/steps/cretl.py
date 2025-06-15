# plugins/steps/cretl.py

import pandas as pd
from sqlalchemy import Table, MetaData, Column, Integer, Float, String, BIGINT, DateTime, Boolean, UniqueConstraint, inspect
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_table():
    """
    #### Create table
    """
    hook = PostgresHook('destination_db')
    db_conn = hook.get_sqlalchemy_engine()
    print('db_conn: ', db_conn)
            
    metadata = MetaData()
    alt_users_churn = Table(
        'cost_estimate',
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
        UniqueConstraint('id', name='unique_flats_constraint')
    ) 
    
    if not inspect(db_conn).has_table(alt_users_churn.name): 
        metadata.create_all(db_conn)

def extract(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = f"""
    SELECT 
        f.id, 
        f.floor, 
        f.is_apartment, 
        f.kitchen_area, 
        f.living_area, 
        f.rooms, 
        f.studio, 
        f.total_area, 
        f.price, 
        f.building_id,
        b.build_year,
        b.building_type_int,
        b.latitude,
        b.longitude,
        b.ceiling_height,
        b.flats_count,
        b.floors_total,
        b.has_elevator
    FROM public.flats f
    left join buildings b 
    on f.building_id = b.id
    """
    data = pd.read_sql(sql, conn)
    ti.xcom_push(key='extracted_data', value=data)
    conn.close()

def type_build(value):
    if value <= 3:
        return "low_rise"
    if 3 < value <= 5:
        return "mid_rise"
    elif 5 < value <= 9:
        return "multy_story"
    elif 9 < value <= 25:
        return "high_rise"

def transform(**kwargs):
    """
    #### Transform task
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
    print(type(data))
    data['is_apartment'] = data['is_apartment'].astype(int)
    data['studio'] = data['studio'].astype(int)
    data['has_elevator'] = data['has_elevator'].astype(int)
    data['build_type_floors'] = data['floors_total'].map(type_build)
    data.loc[data['floors_total']*data['ceiling_height'] >= 150, 'build_type_floors'] = 'skyscraper'
    ti.xcom_push(key='transformed_data', value=data)
    

def load(**kwargs):
    """
    #### Load task
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="cost_estimate",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist()
)