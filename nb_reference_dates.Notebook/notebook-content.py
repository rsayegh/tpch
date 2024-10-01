# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "52ef2901-d0e7-4006-b0d4-e90269a3d07f",
# META       "default_lakehouse_name": "lakehouse01",
# META       "default_lakehouse_workspace_id": "1de9f7f6-122d-4b31-bc96-f2fbd2bac98c",
# META       "known_lakehouses": [
# META         {
# META           "id": "52ef2901-d0e7-4006-b0d4-e90269a3d07f"
# META         },
# META         {
# META           "id": "388d7a4e-cf34-46ab-b570-75b5b4a1c0e6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Notebook parameters**


# PARAMETERS CELL ********************

pStartDate = '2023-01-01'
pEndDate = '2023-12-31'
pLoadId = 2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Spark configuration**

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
# import datetime
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import numpy as np
from datetime import datetime, timedelta, time
import sys
from timeit import default_timer as timer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Time range**

# CELL ********************

try:
    # get the start date from query_start_report
    vStartDateQuery = f"SELECT CAST('{pStartDate}' AS DATE) AS Dates"
    vEndDateQuery = f"SELECT CAST('{pEndDate}' AS DATE) AS Dates"
    dfStartDate = spark.sql(vStartDateQuery)
    dfEndDate = spark.sql(vEndDateQuery)
    dfDate = dfStartDate.union(dfEndDate)

    # # convert to pandas
    dfDate = dfDate.toPandas()

    # put the min and max dates in variables, add one day to the endDate
    startDate = dfDate['Dates'].min()
    endDate = dfDate['Dates'].max() #+ datetime.timedelta(days=1)

except Exception as e:
    vMessage = "failed - setting time range"
    print(f"{vMessage}. exception", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Function to generate the dates**

# CELL ********************

# function that takes a start and end date to genereate a series of dates with an interval
def generate_series(start, stop, interval):
    """
    :param start  - lower bound, inclusive
    :param stop   - upper bound, exclusive
    :interval int - increment interval in seconds
    """

    spark = SparkSession.builder.getOrCreate()

    # Determine start and stops in epoch seconds
    start, stop = spark.createDataFrame(
        [(start, stop)], ("start", "stop")
    ).select(
        [col(c).cast("timestamp").cast("long") for c in ("start", "stop")
    ]).first()
    # Create range with increments and cast to timestamp
    return spark.range(start, stop, interval).select(
        col("id").cast("timestamp").alias("DateAndTime")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Reference dates**

# CELL ********************

# start timer 
start = timer()

try:
    # generate the date series with an interval of 15min 
    dfSeries = generate_series(startDate, endDate, 60 * 15)

    # convert to pandas
    dfSeries = dfSeries.toPandas()

    # enrich the table with additional columns
    dfSeries['Date'] = dfSeries['DateAndTime'].dt.date
    dfSeries['DateKey'] = dfSeries['DateAndTime'].dt.strftime('%Y%m%d').astype(int)
    dfSeries['MonthKey'] = dfSeries['DateAndTime'].dt.strftime('%Y%m%d').astype(str).str[:6]
    dfSeries['Year'] = dfSeries['DateAndTime'].dt.year
    dfSeries['Quarter'] = 'Q' + dfSeries['DateAndTime'].dt.quarter.astype(str)
    dfSeries['Month'] = dfSeries['DateAndTime'].dt.month
    dfSeries['Day'] = dfSeries['DateAndTime'].dt.day
    dfSeries['IsWholeHour'] = np.where(dfSeries['DateAndTime'].dt.strftime('%M') == '00' , True, False)

    # perform the conversion of columns
    dfSeries = dfSeries.astype({
            "DateKey" : "int32",
            "MonthKey" : "int32",
            "Year": "int32",
            "Month": "int32",
            "Day": "int32"
        })

    # convert from pandas to spark 
    sparkDF = spark.createDataFrame(dfSeries) 

    # save to target
    sparkDF.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/date")


except Exception as e:
    vMessage = "failed - reference dates"
    print(f"{vMessage}. exception", str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
