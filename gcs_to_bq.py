from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.bigquery import BigQueryWarehouse
import shutil

@task(retries=3)
def create_external_table(years):
    """
    creates an external table from the data in GCS
    """
    mapped_years = list(map(lambda year: f'gs://de_toronto_covid_de-project-akshar/data/{year}/covid-19-toronto-*-{year}.parquet', years))
    print(f"Years are mapped as {mapped_years}")
    with BigQueryWarehouse.load('de-project-bigquery-prefect-akshar') as warehouse: # your block name in load method
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
    """
    Creates a partitioned table with Episode_Date as partioning column
    """
    with BigQueryWarehouse.load('de-project-bigquery-prefect-akshar') as warehouse: # your block name in load method
        operation = """
            CREATE OR REPLACE TABLE `de-project-akshar.toronto_covid_data.ext_partitioned_coviddata`
            PARTITION BY DATE(`Episode_Date`) AS (
            SELECT * FROM `de-project-akshar.toronto_covid_data.external_coviddata`);
            """
        warehouse.execute(operation)
    return    

@task(retries=3)
def create_external_partitioned_clustered_table():
    """
    Creates a partitioned table with clustering
    Partition column: Episode_Date
    Cluster column: Assigned_ID
    """
    with BigQueryWarehouse.load('de-project-bigquery-prefect-akshar') as warehouse: # your block name in load method
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
    """
    Main ETL flow to load data from GCS into Big Query;
    Orchestration will be controlled via params as well
    """
    # Unlike the way taught in the zoomcamp, 
    # I decided to create the external table(s) directly via the prefect_gcp bigquery block
    # This way I avoided saving data locally and also did things more programatically 
    print("Creating External Table...")
    create_external_table(years)
    # print("Creating Partitioned Table...") # Not necessary
    # create_external_partitioned_table()
    print("Creating Partitioned + Clustered Table...")
    create_external_partitioned_clustered_table()
    # Note that we only require the first and the third table created as that's the most optimized one
    # We can still create other (non-optimized) external tables to compare their performances

if __name__ == "__main__":
    years = [2020 + i for i in range(4)]
    etl_gcs_to_bq(years)
