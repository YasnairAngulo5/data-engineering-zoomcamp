from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import json
import os
from pathlib import Path

file_path = Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "service_account_info.json"))

#GCP Credentials Blocks
with open(file_path) as f:
    service_account_info = json.load(f)

GcpCredentials(
    service_account_info=service_account_info
).save("zoom-gcp-creds")


#GCP Storage Block
bucket_block = GcsBucket(
gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
bucket="prefect-de-zoomcamp_yas",
)

bucket_block.save("zoom-gcs", overwrite=True)