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
		
blob_service_client = BlobServiceClient(account_url="https://test.blob.core.windows.net", credential='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
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

# Write the DataFrame to a table
df1.write.mode("overwrite").saveAsTable("dmi_mshedw_qa_sandbox.test.appointments")
df2.write.mode("overwrite").saveAsTable("dmi_mshedw_qa_sandbox.test.clients")
df5.write.mode("overwrite").saveAsTable("dmi_mshedw_qa_sandbox.test.purchases")
df6.write.mode("overwrite").saveAsTable("dmi_mshedw_qa_sandbox.test.services")

# COMMAND ----------

df_app = spark.sql('''select count(*) from dmi_mshedw_qa_sandbox.test.appointments''')
df_app.show()
df_pur = spark.sql('''select count(*) from dmi_mshedw_qa_sandbox.test.purchases''')
df_pur.show()
df_svcs = spark.sql('''select count(*) from dmi_mshedw_qa_sandbox.test.services''')
df_svcs.show()
df_clts = spark.sql('''select count(*) from dmi_mshedw_qa_sandbox.test.clients''')
df_clts.show()


# COMMAND ----------

# Test script with single clientid

dftest = spark.sql('''with test as (
select cl.id as clientid,  sum(pur.loyalty_points) loyalty_points,cl.first_name fname, cl.last_name lname
from dmi_mshedw_qa_sandbox.test.clients cl
join dmi_mshedw_qa_sandbox.test.appointments app on app.client_id = cl.id
join dmi_mshedw_qa_sandbox.test.purchases pur on pur.appointment_id = app.id
where app.start_time >= '2018-01-01 00:00:00 +0000' and cl.banned = 'FALSE'
and cl.id = 'e0779fa6-7635-4df6-b906-d0d665ce5044'
group by cl.id,cl.first_name, cl.last_name
union all
select cl.id as clientid,  sum(svcs.loyalty_points) loyalty_points,cl.first_name fname, cl.last_name lname
from dmi_mshedw_qa_sandbox.test.clients cl
join dmi_mshedw_qa_sandbox.test.appointments app on app.client_id = cl.id
join dmi_mshedw_qa_sandbox.test.services svcs on svcs.appointment_id = app.id
where app.start_time >= '2018-01-01 00:00:00 +0000' and  cl.banned = 'FALSE'
and cl.id = 'e0779fa6-7635-4df6-b906-d0d665ce5044'
group by cl.id,cl.first_name, cl.last_name)

select clientid,loyalty_points,fname,lname,rnk
from(
select clientid,sum(loyalty_points) as loyalty_points,fname,lname, rank() over (order by sum(loyalty_points) desc) as rnk
from test
group by clientid,fname,lname)
where rnk<=50''')

# COMMAND ----------

dftest.show(10)

# COMMAND ----------

# Final script to identify the top 50 clients with most loyalty points

dffinal = spark.sql('''with test as (
select cl.id as clientid,  sum(pur.loyalty_points) loyalty_points,cl.first_name fname, cl.last_name lname
from dmi_mshedw_qa_sandbox.test.clients cl
join dmi_mshedw_qa_sandbox.test.appointments app on app.client_id = cl.id
join dmi_mshedw_qa_sandbox.test.purchases pur on pur.appointment_id = app.id
where app.start_time >= '2018-01-01 00:00:00 +0000' and cl.banned = 'FALSE'
--and cl.id = 'e0779fa6-7635-4df6-b906-d0d665ce5044'
group by cl.id,cl.first_name, cl.last_name
union all
select cl.id as clientid,  sum(svcs.loyalty_points) loyalty_points,cl.first_name fname, cl.last_name lname
from dmi_mshedw_qa_sandbox.test.clients cl
join dmi_mshedw_qa_sandbox.test.appointments app on app.client_id = cl.id
join dmi_mshedw_qa_sandbox.test.services svcs on svcs.appointment_id = app.id
where app.start_time >= '2018-01-01 00:00:00 +0000' and  cl.banned = 'FALSE'
--and cl.id = 'e0779fa6-7635-4df6-b906-d0d665ce5044'
group by cl.id,cl.first_name, cl.last_name)

select clientid,loyalty_points,fname,lname,rnk
from(
select clientid,sum(loyalty_points) as loyalty_points,fname,lname, rank() over (order by sum(loyalty_points) desc) as rnk
from test
group by clientid,fname,lname)
where rnk<=50''')

# COMMAND ----------

dffinal.show(50)
