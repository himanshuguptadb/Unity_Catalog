# Databricks notebook source
# DBTITLE 1,Widgets prep
dbutils.widgets.removeAll()
dbutils.widgets.dropdown(
    "artifact",
    "Jobs",
    [
        "Jobs",
        "Clusters",
        "Cluster_Policies",
        "Pipelines",
        "Pools",
        "SQL_Warehouses",
        #         "MLFlow_Experiments",
        "MLFlow_Models",
        "Repos",
        "Notebooks",
        "Directories",
    ],
    "Type of Artifact",
)
dbutils.widgets.text(
    "table_name",
    "hive_metastore.default.uc_group_permissions",
    "Name of the Permissions Table",
)

# COMMAND ----------

token = (
    dbutils.widgets._entry_point.getDbutils().notebook().getContext().apiToken().get()
)

# COMMAND ----------

# DBTITLE 1,Imports
import requests as req
import pandas as pd
import json
from pyspark.sql import functions as f
from pyspark.sql import types as t
import base64
from pprint import pprint
import ast

# COMMAND ----------

# DBTITLE 1,Running the utilities in the background
# MAGIC %run ./utilities

# COMMAND ----------

# DBTITLE 1,Initializing the required variables
table_name = dbutils.widgets.get("table_name")
type_of_permission_migration = dbutils.widgets.get("artifact")
context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
instancename = context["tags"]["browserHostName"]
category = apply_category_dict[type_of_permission_migration]

perm_uri = perm_uri_dict[type_of_permission_migration]
print(perm_uri)

# COMMAND ----------

# DBTITLE 1,Functions to update permissions
@f.udf(t.StringType())
def update_permits(artifact_id, grp_name, permission):
    uri = perm_uri.format(instancename=instancename, artifact_id=artifact_id)
    payload = json.dumps(
        {
            "access_control_list": [
                {"group_name": grp_name, "permission_level": permission}
            ]
        }
    )
    job_perm = req.patch(uri, headers=api_header, data=payload)
    if job_perm.status_code == 200:
        return "Permission set Successfully"
    else:
        return f"Error: {job_perm.content}"

# COMMAND ----------

# DBTITLE 1,Reflecting the updated permissions
if spark.catalog.tableExists(table_name):
    adf = spark.table(table_name)
    adf = adf.filter(f.col("artifact_type") == category)
    adf = adf.repartition(1).repartition(worker_cores)
    adf = adf.withColumn(
        "permission_update_status",
        update_permits("id", "new_group_names", "permission_level"),
    )
    adf.write.format("delta").mode("overwrite").partitionBy("artifact_type").option(
        "partitionOverwriteMode", "dynamic"
    ).saveAsTable(table_name)
    adf = spark.table(table_name).filter(f"""artifact_type == "{category}" """)
    adf.display()
else:
    print("Execute notebook 1 first!!")
