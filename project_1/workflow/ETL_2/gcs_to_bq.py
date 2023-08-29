import datetime
import os
from pathlib import Path
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


'''
This file Downloads data from NY_TAXI, Transforms it and Uploads it to a Google Cloud Data Lake (GCS Bucket).
The idea is to simulate a real use case of a ETL workflow
'''

MAP_PAYMENT_TYPE_DESCRIPTION = {
    1: 'Credit card',
    2: 'Cash',
    3: 'No charge',
    4: 'Dispute',
    5: 'Unknown',
    6: 'Voided trip'
}


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_from_gcs(color: str, year: int, month: int) -> Path:
    # This is done to get the correct url
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=".")
    return Path(f"{gcs_path}")


@task(log_prints=True)
def extract(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    # As seen in the notebook, when the passanger count is null there are other attributes that are null. (In both green and yellow taxis)
    # Therefore, i will remove those instances
    # I could add more transformations here, this is just an easy use case.
    df = df[~df['passenger_count'].isnull()]
    # I could create a hash that sets the payment_type_description here.
    df['payment_type_description'] = df['payment_type'].map(
        MAP_PAYMENT_TYPE_DESCRIPTION)
    return df


@task(log_prints=True)
def load(df: pd.DataFrame, project_id: str, table: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table=table,
        project_id=project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Data")
def main_flow(
        months: list[int] = [1, 2], years: list[int] = [2021, 2022], colors: list[str] = ["green", "yellow"], project_id: str = 'ageless-aura-391117', table: str = 'trips_data_all.rides'
):
    for color in colors:
        table_name = f'{table}_{color}'
        log_subflow(table_name)
        for year in years:
            for month in months:
                path = download_from_gcs(color, year, month)
                raw_df = extract(path)
                transformed_df = transform(raw_df)
                load(transformed_df, project_id, table_name)


if __name__ == '__main__':
    main_flow()
