# Project 1
#### I am using Terraform, GCP, Prefect
#### This project is based in https://github.com/DataTalksClub/data-engineering-zoomcamp

The idea is to take some parquet files from the "New York City TLC Trip Record Data" thats publicly available from this link https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet
At first, i will extract and put it in a Data Lake (GCS).
Then, i will add that data into a Data Warehouse (BigQuery).

I will be using Terraform to set up the infrastructure in GCP.