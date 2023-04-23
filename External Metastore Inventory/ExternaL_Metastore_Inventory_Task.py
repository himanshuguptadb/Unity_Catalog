# Databricks notebook source
dbutils.widgets.text("database", "", "")

database =  dbutils.widgets.get("database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Definitions

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.mapKeyDedupPolicy = LAST_WIN

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col, lit
import pandas as pd
from pyspark.sql import functions as F

#function to merge two dataframes
def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

#function to get database and table 
def table_view_details(table_name):
  managed = "False"
  #is_view = "False"
  #is_delta = "False"
  table_type =  None
  storage_location = None
  storage_format = None
  error = None
  #should_copy = "False"
  
  table_column = StructType([
  StructField('col_name', StringType(), True),
  StructField('data_type', StringType(), True),
  StructField('comment', StringType(), True)
  ])
  
  try:
    tab =  spark.sql(f'DESCRIBE EXTENDED {table_name}').collect()
    df = pd.DataFrame(tab)
    #table_df = spark.createDataFrame(data=tab, schema = table_column)
    #storage_format = table_df.select("data_type").filter(col("col_name") == "Provider").collect()
    if len(df.loc[df[0] == 'Provider']) != 0:
      storage_format = df.loc[df[0] == 'Provider', [1]].values[0][0]
    if len(df.loc[df[0] == 'Type']) != 0:
      table_type = df.loc[df[0] == 'Type', [1]].values[0][0]
    #table_type = table_df.select("data_type").filter(col("col_name") == "Type").collect()
    if len(df.loc[df[0] == 'Is_managed_location']) != 0:
      managed = df.loc[df[0] == 'Is_managed_location', [1]].values[0][0]
    #managed = table_df.select("data_type").filter(col("col_name") == "Is_managed_location").collect()
    if len(df.loc[df[0] == 'Location']) != 0:
      storage_location = df.loc[df[0] == 'Location', [1]].values[0][0]
    #storage_location = table_df.select("data_type").filter(col("col_name") == "Location").collect()
    #if r['col_name'] == 'Provider' and r['data_type'] == 'delta':
    #    is_delta = "True"
    #  if r['col_name'] == 'Provider':
    #    storage_format = r['data_type']
    #  if r['col_name'] == 'Type' and r['data_type'] == 'VIEW':
    #    is_view = "True"
    #  if r['col_name'] == 'Is_managed_location' and r['data_type'] == 'true':
    #    managed = "True"
    #  if r['col_name'] == 'Location':
    #    storage_location = r['data_type']
    #is_root_storage = storage_location is not None and (storage_location.startswith('dbfs:/') or storage_location.startswith('wasb')) and not storage_location.startswith('dbfs:/mnt/') 
    #if is_root_storage == "True" or managed == "True":
    #  should_copy = "True"
  except Exception as e:
    error = str(e)
    #return storage_location,storage_format, is_view, is_delta, should_copy, error    
  
  return storage_location,storage_format, table_type, managed, error

def metadata_query(database_to_upgrade):
  
  """
  Args:
      database_to_upgrade (str):          database source to upgrade (in hive_metastore)
  """      
  print(f'Querying database under `hive_metastore`.`{database_to_upgrade}`.')
  #Now iterate over all the tables or views with the database
  table_details_Columns = StructType([
  StructField('database', StringType(), True),
  StructField('tableName', StringType(), True),
  StructField('isTemporary', StringType(), True),
  StructField('Type', StringType(), True),
  StructField('Provider', StringType(), True),
  StructField('Location', StringType(), True),
  StructField('Serde Library', StringType(), True),
  StructField('Error', StringType(), True)
  ])
  #table_details_Columns = ["database_name","table_name","is_view","is_delta","storage_location","should_copy","error"]
  #db_table_details = spark.createDataFrame([], schema = table_details_Columns)
  
  metadata = spark.sql(f"SHOW TABLE EXTENDED  IN {database_to_upgrade} LIKE '*'")

  keys = ["Type", "Provider", "Location", "Serde Library"]

  metadata_parsed = metadata.withColumn('information_detail', F.expr("str_to_map(information,'\n',':')"))\
                            .selectExpr("*", *[ f"information_detail['{k}'] as `{k}`" for k in keys ])\
                            .drop("information")\
                            .drop("information_detail")\
  
  if metadata_parsed.count() == 0:
    metadata_parsed = []
    metadata_parsed = spark.createDataFrame(data=[(database_to_upgrade,"","","","","","","Empty Database")], schema = table_details_Columns)
  #   db_table_details = unionAll(db_table_details, no_tables_DF)
  # else:
  #   for row in tables:
  #     table_name = row['tableName']
  #     full_table_name_source = f'`hive_metastore`.`{database_to_upgrade}`.`{table_name}`'
  #     storage_location,storage_format, table_type, managed, error = table_view_details(full_table_name_source)

  #     table_detailsDF = spark.createDataFrame(data=[(database_to_upgrade,table_name,table_type,storage_format,managed,storage_location,error)], schema = table_details_Columns)
  #     db_table_details = unionAll(db_table_details, table_detailsDF)

  return metadata_parsed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Script

# COMMAND ----------

metadata_status = metadata_query(database_to_upgrade = database)
#display(metadata_status)
location = "/tmp/ExternaL_Metastore_Inventory_Job/"+database
metadata_status.coalesce(1).write.parquet(location)
#metadata_status.write.mode("append").format("delta").saveAsTable("himanshu_gupta_demos.uc_upgrade.metastore_inventory_status1")
#metadata_status_j = metadata_status.toPandas().to_json(orient='records')
#dbutils.notebook.exit(metadata_status_j)
dbutils.notebook.exit("1")


# COMMAND ----------


