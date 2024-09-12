# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7da7e727-e309-41d2-b48a-819835ce9a5d",
# META       "default_lakehouse_name": "tpchlake",
# META       "default_lakehouse_workspace_id": "16e021f2-180b-4bb2-9e48-32981250ce0a",
# META       "known_lakehouses": [
# META         {
# META           "id": "7da7e727-e309-41d2-b48a-819835ce9a5d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Install duckdb**

# CELL ********************

!pip install duckdb --pre --upgrade
!pip install deltalake

# MARKDOWN ********************

# **tpch size**

# CELL ********************

sf =100

# MARKDOWN ********************

# **Import the data into the File folder**

# CELL ********************

# MAGIC %%time
# MAGIC 
# MAGIC import duckdb
# MAGIC import pathlib
# MAGIC 
# MAGIC for x in range(0, sf):
# MAGIC 
# MAGIC     con=duckdb.connect()
# MAGIC 
# MAGIC     con.sql('PRAGMA disable_progress_bar;SET preserve_insertion_order=false')
# MAGIC 
# MAGIC     con.sql(f"CALL dbgen(sf={sf} , children ={sf}, step = {x})") 
# MAGIC 
# MAGIC     for tbl in ['nation','region','customer','supplier','lineitem','orders','partsupp','part']:
# MAGIC 
# MAGIC         pathlib.Path(f'/lakehouse/default/Files/{sf}/{tbl}').mkdir(parents=True, exist_ok=True) 
# MAGIC         con.sql(f"COPY (SELECT * FROM {tbl}) TO '/lakehouse/default/Files/{sf}/{tbl}/{x:02d}.parquet' ")
# MAGIC 
# MAGIC     con.close()

# MARKDOWN ********************

# **Configure the session**

# CELL ********************

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

# MARKDOWN ********************

# **Load into Delta tables**

# CELL ********************

from pyspark.sql.types import *
def loadFullDataFromSource(table_name):
    df = spark.read.parquet(f'Files/{sf}/' + table_name + '/*.parquet')
    df.write.mode("overwrite").format("delta").save(f"Tables/" + table_name)

full_tables = [
    'customer',
    'lineitem',
    'nation',
    'orders' ,
    'region',
    'partsupp',
    'supplier' ,
    'part'
    ]

for table in full_tables:
    loadFullDataFromSource(table)

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import year
import pandas as pd


df = spark.read.parquet(f'Files/100/lineitem/*.parquet')
df_new = df.withColumn("commit_year", year(df['l_commitdate']))
df_new.write.mode("overwrite").format("delta").partitionBy("commit_year").save('Tables/lineitem_partitioned')
