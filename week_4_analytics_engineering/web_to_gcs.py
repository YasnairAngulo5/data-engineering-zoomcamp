import os
import pandas as pd
import logging
import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage

"""
Pre-reqs: 
1. `pip3 install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_dtc-de-375507")

##
#  Schemas
table_schema_green = pa.schema(
    [
        ('VendorID',pa.string()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)

table_schema_yellow = pa.schema(
   [
        ('VendorID', pa.string()), 
        ('tpep_pickup_datetime', pa.timestamp('s')), 
        ('tpep_dropoff_datetime', pa.timestamp('s')), 
        ('passenger_count', pa.int64()), 
        ('trip_distance', pa.float64()), 
        ('RatecodeID', pa.string()), 
        ('store_and_fwd_flag', pa.string()), 
        ('PULocationID', pa.int64()), 
        ('DOLocationID', pa.int64()), 
        ('payment_type', pa.int64()), 
        ('fare_amount',pa.float64()), 
        ('extra',pa.float64()), 
        ('mta_tax', pa.float64()), 
        ('tip_amount', pa.float64()), 
        ('tolls_amount', pa.float64()), 
        ('improvement_surcharge', pa.float64()), 
        ('total_amount', pa.float64()), 
        ('congestion_surcharge', pa.float64())]

)

table_schema_fhv = pa.schema(
   [    
        ('dispatching_base_num', pa.string()),
        ('pickup_datetime', pa.timestamp('s')),
        ('dropOff_datetime', pa.timestamp('s')),
        ('PUlocationID', pa.int64()),
        ('DOlocationID', pa.int64()),
        ('SR_Flag', pa.int64()),
        ('Affiliated_base_number', pa.string())
    ]

)

def format_to_parquet(df: pd.DataFrame, service, src_file):
    
    table = pa.Table.from_pandas(df)

    if service == 'yellow':
        table = table.cast(table_schema_yellow)
    
    elif service == 'green':
        table = table.cast(table_schema_green)
    
    elif service == 'fhv':
        table = table.cast(table_schema_fhv)

    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file, timeout=120)
    


def web_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]
         # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}" 
        logging.info(f"URL: {request_url}")
        df = pd.read_csv(request_url)
    
        
        # read it back into a parquet file
        parquetized = format_to_parquet(df, service, file_name )
        file_name = file_name.replace('.csv.gz', '.parquet')
        #df.to_parquet(file_name, compression="gzip")
        print(f"Parquet: {file_name}")
        
        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")
        
        


#web_to_gcs('2019', 'green')
#web_to_gcs('2020', 'green')
#web_to_gcs('2019', 'yellow')
#web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')