import os
from pathlib import Path
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

'''
This file gets data from a Google Bucket and loads it to a Data Warehouse
The idea is to simulate a real use case of a ELT workflow
'''


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(color: str, year: int, month: int) -> pd.DataFrame:
    # This is done to get the correct url
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    parquet_url = f'{url}/{color}_tripdata_{year}-{month:02}.parquet'
    df = pd.read_parquet(parquet_url)
    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Since i am updating to a Data Lake, i should only update dtypes here.
    # As seen in the notebook, there is none. So there is no transformation needed for now
    return df


@task()
def write_local(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame out locally"""
    path = Path(f"data/{filename}")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def load(path: Path) -> None:
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@task(log_prints=True)
def remove(path: Path) -> None:
    if os.path.exists(path):
        os.remove(path)
        print(f'Removed file: {path}')
    else:
        print(f'File does not exist {path}')


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
                filename = f'{color}/{color}_tripdata_{year}-{month:02}.parquet'
                raw_df = extract(color, year, month)
                transformed_df = transform(raw_df)
                path = write_local(transformed_df, filename)
                load(path)
                remove(path)


if __name__ == '__main__':
    main_flow()
