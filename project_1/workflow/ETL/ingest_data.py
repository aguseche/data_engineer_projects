import datetime
import os
from time import time
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.utilities.logging import log_task

'''
This file Downloads data from NY_TAXI, Transforms it and Uploads it to a Postgres Database that runs in a docker container.
The idea is to simulate a real use case of a ETL workflow
'''


@task(log_prints=True, retries=3)
def extract(url: str, color: str, year: int, month: int) -> pd.DataFrame:
    # This is done to get the correct url
    if month < 10:
        month = '0' + str(month)
    parquet_url = f'{url}/{color}_tripdata_{year}-{month}.parquet'
    df = pd.read_parquet(parquet_url)
    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # As seen in the notebook, when the passanger count is null there are other attributes that are null. (In both green and yellow taxis)
    # Therefore, i will remove those instances
    # I could add more transformations here, this is just an easy use case.
    df = df[~df['passenger_count'].isnull()]
    return df


@task(log_prints=True)
def load(table_name: str, df: pd.DataFrame) -> None:
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as connection:
        engine = connection.engine
        existing_tables = engine.table_names()
        if table_name not in existing_tables:
            df.head(n=0).to_sql(name=table_name,
                                con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Data")
def main_flow():
    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    MONTHS = 2
    YEARS = [2021, 2022]
    COLORS = ['green', 'yellow']
    for color in COLORS:
        table_name = f'ny_taxi_trips_{color}'
        log_subflow(table_name)
        for year in YEARS:
            for month in range(1, MONTHS+1):
                raw_df = extract(URL, color, year, month)
                transformed_df = transform(raw_df)
                load(table_name, transformed_df)


if __name__ == '__main__':
    main_flow()
