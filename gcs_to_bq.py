from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_gcp import GcpCredentials
import shutil

# @task(retries=3)
# def extract_from_gcs(year: int, month: int) -> Path:
#     """Download trip data from GCS"""
#     gcs_path = f"data/{year}/covid-19-toronto-{month:02}-{year}.parquet"
#     gcs_block = GcsBucket.load("de-project-2023-prefect-block")
#     gcs_block.get_directory(from_path=gcs_path, local_path=".")
#     return Path(f"./{gcs_path}")

# @task(retries=3)
# def write_bq(df: pd.DataFrame) -> None:
#     """Write DataFrame to BiqQuery"""
#     gcp_credentials_block = GcpCredentials.load("de-project-gcp-creds")
    # bigquery_block = BigQueryWarehouse.load('de-project-bigquery-prefect-akshar')

    # df.to_gbq(
    #     destination_table="toronto_covid_data.toronto_2022_data",
    #     project_id="de-project-akshar",
    #     credentials=gcp_credentials_block.get_credentials_from_service_account(),
    #     chunksize=100_000,
    #     if_exists="append"
    # )

@task(retries=3)
def create_external_table(years):
    mapped_years = list(map(lambda year: f'gs://de_toronto_covid_de-project-akshar/data/{year}/covid-19-toronto-*-{year}.parquet', years))
    print(f"Years are mapped as {mapped_years}")
    with BigQueryWarehouse.load('de-project-bigquery-prefect-akshar') as warehouse:
        # operation = """
        # CREATE OR REPLACE EXTERNAL TABLE `de-project-akshar.toronto_covid_data.external_coviddata`
        #     OPTIONS (
        #     format = 'PARQUET',
        #     uris = ['gs://de_toronto_covid_de-project-akshar/data/2020/covid-19-toronto-*-2020.parquet',
        #     'gs://de_toronto_covid_de-project-akshar/data/2021/covid-19-toronto-*-2021.parquet',
        #     'gs://de_toronto_covid_de-project-akshar/data/2022/covid-19-toronto-*-2022.parquet',
        #     'gs://de_toronto_covid_de-project-akshar/data/2023/covid-19-toronto-*-2023.parquet']]
        #     );
        # """
        operation = f"""
        CREATE OR REPLACE EXTERNAL TABLE `de-project-akshar.toronto_covid_data.external_coviddata`
            OPTIONS (
            format = 'PARQUET',
            uris = {mapped_years}
            );
        """
        warehouse.execute(operation)
        
    return

@task(retries=3)
def create_external_partitioned_table():
    
    with BigQueryWarehouse.load('de-project-bigquery-prefect-akshar') as warehouse:
        operation = """
            CREATE OR REPLACE TABLE `de-project-akshar.toronto_covid_data.ext_partitioned_coviddata`
            PARTITION BY DATE(`Episode_Date`) AS (
            SELECT * FROM `de-project-akshar.toronto_covid_data.external_coviddata`);
            """
        warehouse.execute(operation)
    return    

@task(retries=3)
def create_external_partitioned_clustered_table():
    with BigQueryWarehouse.load('de-project-bigquery-prefect-akshar') as warehouse:
        operation = """
            CREATE OR REPLACE TABLE `de-project-akshar.toronto_covid_data.external_optimized_covid_toronto_data`
            PARTITION BY DATE(`Episode_Date`) 
            CLUSTER BY Assigned_ID AS (
            SELECT * FROM `de-project-akshar.toronto_covid_data.external_coviddata`);
            """
        warehouse.execute(operation)
    return    
            
@flow(log_prints=True)
def etl_gcs_to_bq(years:list[int] = [2020, 2021]):
    """Main ETL flow to load data into Big Query"""
    print("Creating External Table...")
    create_external_table(years)
    # print("Creating Partitioned Table...")
    # create_external_partitioned_table()
    print("Creating Partitioned + Clustered Table...")
    create_external_partitioned_clustered_table()
    

if __name__ == "__main__":
    years = [2020 + i for i in range(4)]
    etl_gcs_to_bq(years)
