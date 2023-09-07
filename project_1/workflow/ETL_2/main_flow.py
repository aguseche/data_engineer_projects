from prefect import flow

from gcs_to_bq import gcs_to_bq
from web_to_gcs import web_to_gcs

'''
This is the main file of the ETL proccess
It runs the following flows in order: web_to_gcs and gct_to_bq
'''


@flow(name="Main ETL Flow")
def main_flow(
        months: list[int], years: list[int], colors: list[str], project_id: str, table: str
):
    web_to_gcs(months, years, colors)
    # I should add a conditional here so gcs_to_bq only runs with the year-month that was successfully loaded to the Data Lake
    gcs_to_bq(months=months, years=years, colors=colors,
              project_id=project_id, table=table)


# if __name__ == '__main__':
#     months = [1, 2]
#     years = [2021, 2022]
#     colors = ["green", "yellow"]
#     project_id = 'ageless-aura-391117'
#     table = 'trips_data_all.rides'
#     main_flow(months=months, years=years, colors=colors,
#               project_id=project_id, table=table)
