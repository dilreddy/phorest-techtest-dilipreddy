# Databricks notebook source
# MAGIC %pip install azure-storage-blob azure-identity

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql import SQLContext
import os, uuid
#import pysftp
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


account_url = "https://<storageaccountname>.blob.core.windows.net"
default_credential = DefaultAzureCredential()
		
blob_service_client = BlobServiceClient(account_url="https://test.blob.core.windows.net", credential='xxxxxxxxxxxxxx')
# Get a client to interact with a specific container - though it may not yet exist
container_client = blob_service_client.get_container_client("fastauth")

# Create a Spark session
spark = SparkSession.builder.appName("CSVtoTable").getOrCreate()


# Get the absolute path of the CSV file
#csv_path = dbutils.fs.ls(f"wasbs://{container}@stgdatadelivery.blob.core.windows.net/{csv_directory}/{csv_filename}")[0].path
csv_path1 = "abfss://fastauth@dmiprodfastauth.dfs.core.windows.net/test/appointments.csv"
csv_path2 = "abfss://fastauth@dmiprodfastauth.dfs.core.windows.net/test/clients.csv"
csv_path3 = "abfss://fastauth@dmiprodfastauth.dfs.core.windows.net/test/purchases.csv"
csv_path4 = "abfss://fastauth@dmiprodfastauth.dfs.core.windows.net/test/services.csv"

# Read the CSV file into a DataFrame
df1 = spark.read.csv(csv_path1, header=True, inferSchema=True)
df2 = spark.read.csv(csv_path2, header=True, inferSchema=True)
df3 = spark.read.csv(csv_path3, header=True, inferSchema=True)
df4 = spark.read.csv(csv_path4, header=True, inferSchema=True)

# Convert loyalty_points column to long data type
df5 = df3.withColumn("loyalty_points", col("loyalty_points").cast("long"))
df6 = df4.withColumn("loyalty_points", col("loyalty_points").cast("long"))

df5.printSchema()
df5.show()
