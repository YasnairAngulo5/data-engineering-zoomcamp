## To check the size of files
ls -lh 06/

## To copy files from local to gcs
cd data
gsutil -m cp -r pq/ gs://dtc_data_lake_dtc-de-375507/pq

## To install hadoop
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar  gcs-connector-hadoop3-2.2.5.jar 

## To create a local spark cluster
1. Go to the spark installation folder (brew info apache-spark)
2. run ./sbin/start-master.sh