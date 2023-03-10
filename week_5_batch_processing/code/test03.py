import pyspark
from pyspark.sql import types
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

'''
# Part 1 from the video
df =  spark.read.option("header", "true").csv('fhvhv_tripdata_2021-01.csv')
df_pandas = pd.read_csv('head.csv')
# To get a quick view of the data
#spark.createDataFrame(df_pandas).show()

# Trick to get the schema
#print(spark.createDataFrame(df_pandas).schema)
'''
#Part II

schema =  types.StructType([
types.StructField('hvfhs_license_num', types.StringType(), True), 
types.StructField('dispatching_base_num', types.StringType(), True), 
types.StructField('pickup_datetime', types.TimestampType(), True), 
types.StructField('dropoff_datetime', types.TimestampType(), True), 
types.StructField('PULocationID', types.IntegerType(), True), 
types.StructField('DOLocationID', types.IntegerType(), True), 
types.StructField('SR_Flag', types.StringType(), True)
])

df =  spark.read \
.option("header", "true") \
.schema(schema) \
.csv('fhvhv_tripdata_2021-01.csv')

'''
df.show()
print(df.head(10))
'''

df.repartition(24)
df.write.parquet('fhvhv/2021/01/')



