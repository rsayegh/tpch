# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6a07c510-ee0b-4423-b632-d54d7171db26",
# META       "default_lakehouse_name": "tpchlake",
# META       "default_lakehouse_workspace_id": "35721311-739e-48b7-b928-eb4e83de0d3b",
# META       "known_lakehouses": [
# META         {
# META           "id": "6a07c510-ee0b-4423-b632-d54d7171db26"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

!pip install semantic-link-labs --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import sempy.fabric as fabric
import sempy_labs as labs
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaces = ['tpch']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_semantic_models = pd.DataFrame()

for workspace in workspaces:
    df = fabric.list_datasets(workspace=workspace) 
    df.loc[:, 'Workspace Name'] = workspace
    workspace_id = fabric.resolve_workspace_id(workspace=workspace)
    df.loc[:, 'Workspace ID'] = workspace_id
    df_semantic_models = pd.concat([df_semantic_models, df], ignore_index=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

timestamp = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
df_semantic_model_size = pd.DataFrame(columns = ['DatasetsName','WorkspaceName', 'DatasetSize', 'DateTime'])

for _,row in df_semantic_models.iterrows():

    semantic_model_id = row['Dataset ID']
    semantic_model_name = row['Dataset Name']
    workspace_id = row['Workspace ID']
    workspace_name = row['Workspace Name']

    print(f"extracting dataset <{semantic_model_name}> size in workspace <{workspace_name}>")

    try:

        semantic_model_size = labs.get_semantic_model_size(dataset= semantic_model_name, workspace=workspace_name)

        df_semantic_model_size.loc[len(df_semantic_model_size.index)] = [semantic_model_name, workspace_name, semantic_model_size, timestamp]

        print(f"extracting dataset <{semantic_model_name}> size in workspace <{workspace_name}> succeeded")
        print(f"---------------------------------------------------------")   

    except Exception as e:
        print(f"extracting dataset <{semantic_model_name}> size in workspace <{workspace_name}> failed. exception: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_semantic_model_size["DatasetSizeMB"] = df_semantic_model_size["DatasetSize"] / (1024 * 1024)
df_semantic_model_size["DatasetSizeGB"] = df_semantic_model_size["DatasetSize"] / (1024 * 1024 * 1024)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# save dfBpaAssessment to a spark dataframe
if not df_semantic_model_size.empty:
    df_semantic_model_size_ = spark.createDataFrame(df_semantic_model_size)
    path = "abfss://35721311-739e-48b7-b928-eb4e83de0d3b@onelake.dfs.fabric.microsoft.com/6a07c510-ee0b-4423-b632-d54d7171db26/Tables/semantic_model_size"
    df_semantic_model_size_.write.mode("append").format("delta").partitionBy("DateTime").option("mergeSchema", "true").save(path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
