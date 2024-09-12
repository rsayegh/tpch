# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bc01eab2-813e-413b-a59c-f14935b11381",
# META       "default_lakehouse_name": "tpchlake",
# META       "default_lakehouse_workspace_id": "fe2578f5-dccd-4dae-b6d7-5d40405e6150",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Install duckdb**

# CELL ********************

!pip install duckdb --pre --upgrade
!pip install deltalake

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Configure the session**

# CELL ********************

spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **tpch size**

# CELL ********************

sf=100

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
