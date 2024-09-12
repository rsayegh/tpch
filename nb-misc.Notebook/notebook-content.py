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
# META       "default_lakehouse_workspace_id": "16e021f2-180b-4bb2-9e48-32981250ce0a"
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfAccounts = spark.read.format("csv").option("header","true").load("Files/Staging/accounts.csv")
dfAccounts.write.mode("overwrite").format("delta").option("mergeSchema", "true").save("Tables/accounts")

# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/Staging/accounts.csv"
# dfAccounts = pd.read_csv("/lakehouse/default/" + "Files/Staging/accounts.csv")
# dfAccounts.pivot_table(dfAccounts, values="Value", index=["DateKey"], columns=["Account"])
# dfAccounts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime
import numpy as np

# dfAccounts = pd.DataFrame(
#     {
        
#         "Account" : [
#             "Product Revenue",
#             "Service and other revenue",
#             "Revenue",
#             "Product cost",
#             "Service and other costs",
#             "Gross margin",
#             # "Research and development",
#             # "Sales and marketing",
#             # "General and administrative",
#             "Restructuring",
#             "Operating income",
#             "Other Income, net",
#             "Income before income taxes",
#             "Provision for income taxes",
#             "Net income"
#         ]
#         ,
#         "DateKey": [datetime.datetime(2013, i, 1) for i in range(1, 13)] #+ [datetime.datetime(2013, i, 15) for i in range(1, 13)],
#     }
# )

dfAccounts = pd.DataFrame(
    {
        
        "Account" : [
            "Product Revenue",
            "Service and other revenue",
            "Revenue",
            "Product cost",
            "Service and other costs",
            "Gross margin",
            "Research and development",
            "Sales and marketing",
            "General and administrative",
            "Restructuring",
            "Operating income",
            "Other Income, net",
            "Income before income taxes",
            "Provision for income taxes",
            "Net income"
        ],
        "DateKey": [datetime.datetime(2013, 1, 1)] * 15,  # for i in range(1, 13)] + [datetime.datetime(2013, i, 15) for i in range(1, 4)],
        "E": np.random.randint(1, 100) * 15 #np.random.randint(1,100)
    }
)


# dfAccounts.pivot_table(dfAccounts, values="D", index=["A", "B"], columns=["C"])



dfAccounts

# df = pd.DataFrame(
#     {
#         "A": ["one", "one", "two", "three"], # * 6,
#         # "B": ["A", "B", "C"] * 8,
#         # "C": ["foo", "foo", "foo", "bar", "bar", "bar"] * 4,
#         # "D": np.random.randn(24),
#         # "E": np.random.randn(24),
#         "F": [datetime.datetime(2013, i, 1) for i in range(1, 5)]
#         # + [datetime.datetime(2013, i, 15) for i in range(1, 13)],
#     }
# )

# df


# df = pd.DataFrame(
#     {
#         # "A": ["one", "one", "two", "three"] * 6,
#         "A": ["one"] * 3,
#         "B": ["A", "B", "C"],
#         # "C": ["foo", "foo", "foo", "bar", "bar", "bar"] * 4,
#         "D": np.random.randn(3),
#         # "E": np.random.randn(24),
#         "F": datetime.datetime(2013, 1, 1) #[datetime.datetime(2013, i, 1) for i in range(1, 3)]
#         # + [datetime.datetime(2013, i, 15) for i in range(1, 13)],
#     }
# )


# pd.pivot_table(df, values="D", index=["A", "B"], columns=["C"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



import pandas as pd


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfTable1 = spark.read.format("csv").option("header","true").load("Files/Staging/dfTable1.csv")
dfTable1.write.mode("overwrite").format("delta").option("mergeSchema", "true").save("Tables/table1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfTable1 = spark.read.format("csv").option("header","true").load("Files/Staging/dfTable2.csv")
dfTable1.write.mode("overwrite").format("delta").option("mergeSchema", "true").save("Tables/table2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM table1 AS t1
# MAGIC WHERE EXISTS (SELECT Id FROM table2 WHERE t1.Id = Id)
# MAGIC 
# MAGIC -- WITH temp AS(
# MAGIC --     SELECT 
# MAGIC --         table1.Id
# MAGIC --     FROM 
# MAGIC --         table1 
# MAGIC --         INNER JOIN table2 
# MAGIC --             ON table1.Id = table2.Id
# MAGIC -- )
# MAGIC -- DELETE FROM table1
# MAGIC -- WHERE Id IN (
# MAGIC 
# MAGIC -- )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
