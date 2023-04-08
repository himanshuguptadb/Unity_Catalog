# Databricks notebook source
# MAGIC %md
# MAGIC ## Upgrading views from external metastore to UC

# COMMAND ----------

def parallel_sync(database_name):
  print(database_name)
  return dbutils.notebook.run("Metastore_Sync_Views_Task", 60000, {"database": database_name, "catalog": "himanshu_gupta_demos", "owner": "account users" })

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
import concurrent.futures
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import pandas as pd
import json
from datetime import datetime
import time

print('Sync all external databases to the UC:')
#databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
databases = ['000_demo_db','himanshu_gupta_databricks_com']
#upgrades 50 databases in parallel to speedup migration

databases_upgraded = spark.sql(f"select table_schema, table_name  from system.information_schema.tables where table_catalog != 'hive_metastore' and table_schema != 'information_schema'").collect()


syncresultsSchema = StructType([
  StructField('source_schema', StringType(), True),
  StructField('source_name', StringType(), True),
  StructField('source_type', StringType(), True),
  StructField('target_catalog', StringType(), True),
  StructField('target_schema', StringType(), True),
  StructField('target_name', StringType(), True),
  StructField('status_code', StringType(), True),
  StructField('description', StringType(), True)
  ])

syncresultsDF = spark.createDataFrame([], schema = syncresultsSchema)

with ThreadPoolExecutor(max_workers=50) as executor:
  futures = [
        executor.submit(parallel_sync, database) for database in databases
    ]
  
  for future in futures:
        print([f._state for f in futures])
        if future.cancelled():
            continue
        try:
            sync_status = future.result()
            sync_status_df = spark.createDataFrame(pd.read_json(sync_status, orient='columns'))
            syncresultsDF = unionAll(syncresultsDF, sync_status_df)
        except ValueError as e:
            print(f"{datetime.now()} - EXCEPTION! {e}")
            executor.shutdown(wait=False, cancel_futures=True)

print(f"{datetime.now()} - Run complete")

# COMMAND ----------

display(syncresultsDF)
