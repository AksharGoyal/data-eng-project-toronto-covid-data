from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
# from prefect.tasks import task_input_hash
from datetime import datetime#, timedelta

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task()
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # age_range = {f'{num} to {num+9} Years': f'{num}-{num+9}' for num in range(20, 90, 10)}
    # age_range.update({'19 and younger':'19-', '90 and older': '90+'})
    df.drop(['_id'], axis=1, inplace=True)
    # print(f"Columns: {df.columns}")
    # df['Age Group'] = df['Age Group'].map(age_range)
    df['Episode Date'] = pd.to_datetime(df['Episode Date'])
    df['Reported Date'] = pd.to_datetime(df['Reported Date'])
    
    df_clean = df.dropna()
    return df_clean

@task(retries=3)
def write_gcs(path:Path, df: pd.DataFrame=None) -> None:
    """Upload local parquet file to GCS"""
    path = path.as_posix()
    gcs_block = GcsBucket.load("de-project-2023-prefect-block")
    if df is None:
        print("Uploading Locally...")
        gcs_block.upload_from_path(from_path=path, to_path=path, timeout=120)
    else:
        print("Uploading DataFrame...")
        gcs_block.upload_from_dataframe(df=df, to_path=path, timeout=120,
                                   serialization_format='parquet' if path.endswith('parquet') else 'csv_gzip')
    return

@task(retries=3)
def write_local(df: pd.DataFrame, dataset_file:str, year:int, month:int) -> Path:
    """Write DataFrame out locally as parquet file"""

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
    """The main ETL function"""
    dataset_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/64b54586-6180-4485-83eb-81e8fae3b8fe/resource/fff4ee65-3527-43be-9a8a-cb9401377dbc/download/covid19-cases.csv"

    df = fetch(dataset_url)
    df_clean = clean(df)
    # df_clean = df.dropna()
    # write_local(df_clean, "covid-19-toronto")
    # print(path)

    cur_month, cur_year = datetime.now().month, datetime.now().year

    if year == cur_year and month > cur_month:
        print("No more data available...")
        return
    
    temp = df_clean.loc[df_clean['Episode Date'].dt.year.isin([year]) & df_clean['Episode Date'].dt.month.isin([month])]
    path = Path(f"data/{year}/covid-19-toronto-{month:02}-{year}.parquet")
    write_gcs(df=temp, path=path)
    path = Path(f"data/{year}/covid-19-toronto-{month:02}-{year}.csv.gz")
    write_gcs(df=temp, path=path)
    print(f"Written year/month:{year}/{month:02}")

@flow(log_prints=True)
def etl_parent_flow(months: list[int] = [1, 2], years: list[int] = [2020,2021,2022]):
    for year in years:
        for month in months:
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
