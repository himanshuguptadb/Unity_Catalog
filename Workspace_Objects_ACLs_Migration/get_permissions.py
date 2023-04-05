# Databricks notebook source
# MAGIC %pip install databricks_cli

# COMMAND ----------

# DBTITLE 1,Creating the widgets
dbutils.widgets.removeAll()
dbutils.widgets.multiselect(
    "artifacts",
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
# dbutils.widgets.text("token", dbutils.widgets._entry_point.getDbutils().notebook().getContext().apiToken().get(), "Personal Access Token")
dbutils.widgets.text("groups", "None", "List of Groups :: None or String or List")
dbutils.widgets.text(
    "table_name",
    "hive_metastore.default.uc_group_permissions",
    "Name of the Permissions Table",
)
dbutils.widgets.dropdown(
    "save_choice", "No", ["No", "Yes"], "Save the group permissions to table?"
)

# COMMAND ----------

token = (
    dbutils.widgets._entry_point.getDbutils().notebook().getContext().apiToken().get()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### List of Groups can be of the following types
# MAGIC 
# MAGIC * None
# MAGIC * "aa_group"
# MAGIC * ["aa_group","admins","users"]

# COMMAND ----------

# DBTITLE 1,Calling the widgets into variables
import requests as req
import pandas as pd
import json
from pyspark.sql import functions as f
from pyspark.sql import types as t
import base64
from pprint import pprint
import ast

save_choice = dbutils.widgets.get("save_choice")
table_name = dbutils.widgets.get("table_name")
if save_choice == "Yes":
    assert table_name not in [
        None,
        "",
    ], "Table name must be provided if save choice is Yes"

type_of_permission_migration = dbutils.widgets.get("artifacts")
# token = dbutils.widgets.get("token")
context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
instancename = context["tags"]["browserHostName"]
groups_of_interest = ast.literal_eval(dbutils.widgets.get("groups"))
print(
    "Type",
    type_of_permission_migration,
    "\n\ntoken",
    token,
    "\n\ninstancename",
    instancename,
    "\n\ngroups_of_interest",
    groups_of_interest,
    "\n\nsave choice",
    save_choice,
    "\n\ntable name",
    table_name,
    sep="\n",
)

# COMMAND ----------

# DBTITLE 1,Running the utilities in the background
# MAGIC %run ./utilities

# COMMAND ----------

# DBTITLE 1,Groups information filtered or unfiltered based on condition of input
grp_df = get_grp_df().filter("group_name != 'admins' and group_name not like 'uc_%'")
grp_df.display()

# COMMAND ----------

type_of_permission_migration_list = type_of_permission_migration.split(",")
for artifact in type_of_permission_migration_list:
  #Getting the Permissions URI for the particular type of artifact
  perm_uri = perm_uri_dict[artifact]
  print(perm_uri)
  #Preparing the permissions dataframe for the artifact
  artifact_func = perm_data_dict[artifact]
  artifact_list = artifact_func()
  perms = parse_artifact_list(artifact, artifact_list)
  schema = schema_dict[artifact]
  perm_df_func = perm_tranformations_func_dict[artifact]
  perm_df = perm_df_func(perms, schema)
  display(perm_df)
  #perm_df.write.format("delta").mode("append").saveAsTable("himanshu_gupta_demos.uc_upgrade."+table_name+"_All")
  #Joining the permissions back to groups dataframe
  adf = (
    perm_df.alias("perm")
    .join(grp_df.alias("grp"), ["group_name"], "inner")
    .select(
        f.col("perm.name"),
        f.col("perm.id"),
        f.col("group_name").alias("old_group_names"),
        f.col("uc_group_name").alias("new_group_names"),
        f.col("permissions").alias("permission_level"),
        f.col("artifact_type"),
    )
    .withColumn("permission_update_status", f.lit(None).cast("string"))
      )
  adf.display()
  if save_choice == "Yes":
    adf.write.format("delta").mode("overwrite").partitionBy("artifact_type").option("partitionOverwriteMode", "dynamic").saveAsTable(table_name)
