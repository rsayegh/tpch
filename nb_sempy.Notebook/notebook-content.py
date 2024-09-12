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

# CELL ********************

!pip install --upgrade semantic-link --q #upgrade to semantic-link v0.5

import sempy.fabric as fabric
import pandas as pd 




# METADATA ********************

# META {}

# CELL ********************

df = pd.concat([fabric.list_workspaces()], ignore_index=True)
df

# CELL ********************

df = pd.concat([fabric.list_items(workspace=ws) for ws in fabric.list_workspaces().query('`Is On Dedicated Capacity` == True').Id], ignore_index=True)
df

# CELL ********************

# MAGIC %%spark
# MAGIC 
# MAGIC 
# MAGIC import com.databricks.sql.transaction.tahoe._
# MAGIC  
# MAGIC // val databasePath = "abfss://16e021f2-180b-4bb2-9e48-32981250ce0a@onelake.dfs.fabric.microsoft.com/7da7e727-e309-41d2-b48a-819835ce9a5d/Tables/lineitem"
# MAGIC  
# MAGIC // def size(path: String): Long =
# MAGIC //   dbutils.fs.ls(path).map { fi => if (fi.isDir) size(fi.path) else fi.size }.sum
# MAGIC  
# MAGIC // val tables = dbutils.fs.ls(databasePath).par.map { fi =>
# MAGIC //   val totalSize = size(fi.path)
# MAGIC //   val snapshotSize = DeltaLog.forTable(spark, fi.path).snapshot.sizeInBytes
# MAGIC //   (fi.name, totalSize / 1024 / 1024 / 1024, snapshotSize / 1024 / 1024 / 1024)
# MAGIC // }
# MAGIC // display(tables.seq.sorted.toDF("name", "total_size_gb", "snapshot_size_gb"))

# METADATA ********************

# META {
# META   "language": "scala"
# META }
