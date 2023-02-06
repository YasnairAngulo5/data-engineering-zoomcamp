import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    #if randint(0, 1) > 0: #waiting time"
    #    raise Exception

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df= pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # Convert dates fiels to a datatime object
    for col in df.columns:
        if 'datetime' in col:
            df[col]  = pd.to_datetime(df[col])
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out  locally as parquet file"""

    path =  Path(os.path.join(os.path.dirname(os.path.realpath(__file__)),f"data/{color}/{dataset_file}.parquet"))

    try :
        df.to_parquet(path, compression="gzip")
    except OSError as error :
        print(error)
    
    return path

@flow()
def write_gcs(from_path: Path, color: str, dataset_file: str) -> None:
    """Uploading local parquet file to GCS"""
    path_git    = Path(f"data/{color}/{dataset_file}.parquet")
    gcs_block   = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=from_path, to_path=path_git, timeout=120)
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> int:
    """The main ETL fuction"""
    dataset_file    = f"{color}_tripdata_{year}-{month:02}"
    dataset_url     = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df              = fetch(dataset_url)
    df_clean        = clean(df)
    path            = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)
    return len(df_clean)

@flow(log_prints=True)
def etl_main_flow(
    months: list[int] = [1,2], year: int = 2021, color: str = "yellow"
):
    total_rows_processed = 0
    for month in months:
        rows_processed = etl_web_to_gcs(year, month, color)
        total_rows_processed= total_rows_processed + rows_processed

    print(f"Total number of rows processed: {total_rows_processed} ")


if __name__ == '__main__':
    color           = "yellow"
    year            = 2019
    months          = [2,3]
    etl_main_flow(months, year, color)

