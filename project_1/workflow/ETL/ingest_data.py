import datetime
import os
from time import time
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

'''
This file Downloads data from NY_TAXI, Transforms it and Uploads it to a Postgres Database.
The idea is to simulate a real use case of a ETL workflow
'''


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(color: str, year: int, month: int) -> pd.DataFrame:
    # This is done to get the correct url
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
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
def main_flow(
        months: list[int] = [1, 2], years: list[int] = [2021, 2022], colors: list[str] = ["green", "yellow"]
):
    for color in colors:
        table_name = f'ny_taxi_trips_{color}'
        log_subflow(table_name)
        for year in years:
            for month in months:
                raw_df = extract(color, year, month)
                transformed_df = transform(raw_df)
                load(table_name, transformed_df)


if __name__ == '__main__':
    main_flow()
