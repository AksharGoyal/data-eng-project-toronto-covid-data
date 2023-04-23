from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from datetime import datetime

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task()
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the dataset of null values and fixes dtypes of some column"""
    # The _id column is similar to row number and provides no necessary information
    df.drop(['_id'], axis=1, inplace=True)
    # We convert the below variables to datetime to make it easy to partition on them later
    df['Episode Date'] = pd.to_datetime(df['Episode Date'])
    df['Reported Date'] = pd.to_datetime(df['Reported Date'])
    # Drop the null values except for Neighbourhood
    df_clean = df.loc[(~df['Age Group'].isnull()) & (~df['FSA'].isnull())]
    return df_clean

@task(retries=3)
def write_gcs(path:Path, df: pd.DataFrame=None) -> None:
    """Upload the dataframe to GCS"""
    path = path.as_posix()
    # Load the prefect-gcp's cloud storage block
    gcs_block = GcsBucket.load("de-project-2023-prefect-block") # Write your GCS Bucket block name
    # Uploading dataframes from local to GCS, not used
    if df is None:
        print("Uploading Locally...")
        gcs_block.upload_from_path(from_path=path, to_path=path, timeout=120)
    # Uploading dataframes from web to GCS
    else:
        print("Uploading DataFrame...")
        gcs_block.upload_from_dataframe(df=df, to_path=path, timeout=120,
                                   serialization_format='parquet' if path.endswith('parquet') else 'csv_gzip')
    return

@task(retries=3)
def write_local(df: pd.DataFrame, dataset_file:str, year:int, month:int) -> Path:
    """Write DataFrame out locally as parquet file"""
    # This function is not used anymore but is still kept in case needed
    path_file = f"{dataset_file}-{month:02}-{year}.parquet"
    path_dir = Path(f"data/{year}")
    path_dir.mkdir(parents=True, exist_ok=True)
    
    temp = df.loc[df['Episode Date'].dt.year.isin([year]) & df['Episode Date'].dt.month.isin([month])]
    print(f"Shape of data from year:{year} and month:{month} is {temp.shape}")
    if temp.shape[0] > 0:
        # print(path_dir/path_file)
        temp.to_parquet(path_dir / path_file, compression="gzip")
        path_file = f"{dataset_file}-{month:02}-{year}.csv.gz"
        # print(path_dir/path_file)
        temp.to_csv(path_dir / path_file, compression="gzip")
                
    # return path_dir

@flow(log_prints=True)
def etl_web_to_gcs(month:int, year:int) -> None:
    """The main ETL function to orchestrate flow of data from source to GCS"""
    
    # Read the data and clean it
    dataset_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/64b54586-6180-4485-83eb-81e8fae3b8fe/resource/fff4ee65-3527-43be-9a8a-cb9401377dbc/download/covid19-cases.csv"
    df = fetch(dataset_url)
    df_clean = clean(df)
        
    # Instead of uploading the whole dataframe
    # we will divide the data based on years and months to keep it organized
    # Later we can decide which years and months we want during deployment
    temp = df_clean.loc[df_clean['Episode Date'].dt.year.isin([year]) & df_clean['Episode Date'].dt.month.isin([month])]
    # We will save the files as both parquet and csv (with compression)
    path = Path(f"data/{year}/covid-19-toronto-{month:02}-{year}.parquet")
    write_gcs(df=temp, path=path)
    path = Path(f"data/{year}/covid-19-toronto-{month:02}-{year}.csv.gz")
    write_gcs(df=temp, path=path)
    print(f"Written data for year/month:{year}/{month:02}")

@flow(log_prints=True)
def etl_parent_flow(months: list[int] = [1, 2], years: list[int] = [2020,2021,2022]):
    """The parent flow that will be used later for deployment to control the parameters"""
    for year in years:
        for month in months:
            # As we fetching data of all months and all years till now,
            # We use following variables to track when to break the loop
            # to avoid fetching data not available
            cur_month, cur_year = datetime.now().month, datetime.now().year
            if year == cur_year and month > cur_month:
                print("No more data available...")
                return
            etl_web_to_gcs(month, year)
        

if __name__ == "__main__":
    months = (i for i in range(1, 13))
    years = (2020, 2021, 2022, 2023)
    etl_parent_flow(months, years)
    print("DONE!")
