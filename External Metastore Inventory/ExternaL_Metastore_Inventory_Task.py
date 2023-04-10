# Databricks notebook source
dbutils.widgets.text("database", "", "")

database =  dbutils.widgets.get("database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Definitions

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col, lit

#function to merge two dataframes
def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

#function to get database and table 
def table_view_details(table_name):
  managed = "False"
  is_view = "False"
  is_delta = "False"
  storage_location = None
  storage_format = None
  error = None
  should_copy = "False"
  try:
    for r in spark.sql(f'DESCRIBE EXTENDED {table_name}').collect():
      if r['col_name'] == 'Provider' and r['data_type'] == 'delta':
        is_delta = "True"
      if r['col_name'] == 'Provider':
        storage_format = r['data_type']
      if r['col_name'] == 'Type' and r['data_type'] == 'VIEW':
        is_view = "True"
      if r['col_name'] == 'Is_managed_location' and r['data_type'] == 'true':
        managed = "True"
      if r['col_name'] == 'Location':
        storage_location = r['data_type']
    is_root_storage = storage_location is not None and (storage_location.startswith('dbfs:/') or storage_location.startswith('wasb')) and not storage_location.startswith('dbfs:/mnt/') 
    if is_root_storage == "True" or managed == "True":
      should_copy = "True"
  except Exception as e:
    error = str(e)
    return storage_location,storage_format, is_view, is_delta, should_copy, error    
  
  return storage_location,storage_format, is_view, is_delta, should_copy, error

def metadata_query(database_to_upgrade):
  
  """
  Args:
      database_to_upgrade (str):          database source to upgrade (in hive_metastore)
  """      
  print(f'Querying database under `hive_metastore`.`{database_to_upgrade}`.')
  #Now iterate over all the tables or views with the database
  table_details_Columns = StructType([
  StructField('database_name', StringType(), True),
  StructField('table_name', StringType(), True),
  StructField('is_view', StringType(), True),
  StructField('storage_format', StringType(), True),
  StructField('is_delta', StringType(), True),
  StructField('storage_location', StringType(), True),
  StructField('should_copy', StringType(), True),
  StructField('error', StringType(), True)
  ])
  #table_details_Columns = ["database_name","table_name","is_view","is_delta","storage_location","should_copy","error"]
  db_table_details = spark.createDataFrame([], schema = table_details_Columns)
  
  tables = spark.sql(f"SHOW TABLES IN hive_metastore.{database_to_upgrade}").collect()
  
  if len(tables) == 0:
    no_tables_DF = []
    no_tables_DF = spark.createDataFrame(data=[(database_to_upgrade,"","","","","","","Empty Database")], schema = table_details_Columns)
    db_table_details = unionAll(db_table_details, no_tables_DF)
  else:
    for row in tables:
      table_name = row['tableName']
      full_table_name_source = f'`hive_metastore`.`{database_to_upgrade}`.`{table_name}`'
      storage_location,storage_format, is_view, is_delta, should_copy, error = table_view_details(full_table_name_source)

      table_detailsDF = spark.createDataFrame(data=[(database_to_upgrade,table_name,is_view,storage_format,is_delta,storage_location,should_copy,error)], schema = table_details_Columns)
      db_table_details = unionAll(db_table_details, table_detailsDF)
  return db_table_details

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Script

# COMMAND ----------

metadata_status = metadata_query(database_to_upgrade = database)
metadata_status_j = metadata_status.toPandas().to_json(orient='records')
dbutils.notebook.exit(metadata_status_j)
