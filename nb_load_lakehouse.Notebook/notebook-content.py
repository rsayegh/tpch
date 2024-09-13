# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08cd25ee-709e-4a0e-9a12-ec48feb79b48",
# META       "default_lakehouse_name": "raw",
# META       "default_lakehouse_workspace_id": "0fc26c38-56b5-4506-95d6-d24363c97398"
# META     }
# META   }
# META }

# MARKDOWN ********************

# 
# #### Run the cell below to install the required packages for Copilot


# CELL ********************


#Run this cell to install the required packages for Copilot
%pip install https://aka.ms/chat_magics-0.0.0-py3-none-any.whl
%load_ext chat_magics


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Session settings**

# CELL ********************

# v-oder optimization
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# Dynamic Partition Overwrite to avoid deleting existing partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# arrow enablement
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Imports**

# CELL ********************

import pandas as pd
import glob
import pandas as pd
import datetime as dt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to assign a delta table name**

# CELL ********************

def get_delta_name(name):
    match name:           
        case "staging-dimcurrency":
            return "currency"
        case "staging-dimcustomer":
            return "customer"
        case "staging-dimdate":
            return "date"
        case "staging-dimgeography":
            return "geography"
        case "staging-dimproduct":
            return "product"
        case "staging-dimproductcategory":
            return "productcategory"
        case "staging-dimproductsubcategory":
            return "productsubcategory"
        case "staging-dimpromotion":
            return "promotion"
        case "staging-dimsalesterritory":
            return "salesterritory"
        case "staging-factsales":
            return "sales"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Iterate over the shortcuts and load parquet files into the lake house**

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit

pSourceFolder = "Files"
vSubFolders = mssparkutils.fs.ls(pSourceFolder) 


for folderPath in vSubFolders:

    # set variables related to the files
    vShortcutName = folderPath.name
    vShortcutPath = f"Files/{vShortcutName}"  

    # log the current folder 
    print(f"handling {vShortcutName}") 

    # list all files in the folder
    vFiles = mssparkutils.fs.ls(vShortcutPath) 

    # iterate over the files
    for file in vFiles:

        # set the file name
        vFileFullName = file.name
        vFileName = vFileFullName.replace(".parquet", "")

        # get the mapping to the delta table name
        vDeltaTableName = get_delta_name(vFileName)

        print(f"current file is <{vFileName}>") 

        # if vFileName =="staging-dimcurrency":

        # read the parquet file and save as delta table
        try:
        
            # read the parquet file in a pandas dataframe
            df = pd.read_parquet("/lakehouse/default/" + vShortcutPath + "/" + vFileFullName)

            # add a timestamp
            df["CreateDatetime"] = dt.datetime.now()

            # add the original source
            df["Origin"] = vShortcutName
            
            # convert back to spark 
            sparkDF = spark.createDataFrame(df)

            # save the spark dataframe to target by merging the schema
            sparkDF.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(f"{vDeltaTableName}")

            print(f"delta table <{vDeltaTableName}> saved to lakehouse") 

        # catch exception if the try fails
        except Exception as e:
            vMessage = "failed to save delta table <{vDeltaTableName}> lakehouse"
            print(f"{vMessage}. exception: ", str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
