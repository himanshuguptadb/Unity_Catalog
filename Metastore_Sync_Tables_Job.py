# Databricks notebook source
# MAGIC %md
# MAGIC ## Sync sciprt for external tables

# COMMAND ----------

def parallel_sync(database_name):
  print(database_name)
  return dbutils.notebook.run("Metastore_Sync_Tables_as_Task", 60000, {"database": database_name, "catalog": "himanshu_gupta_demos", "owner": "account users", "run": "False" })


def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)


# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
import concurrent.futures
from collections import deque
from functools import reduce
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time


print('Sync all external databases to the UC:')
databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
databases = databases[:1]
#databases = ['ah_feature_store_taxi_demo', 'ahecksher']##, 'ahecksher_dlt_db', 'ahecksherdb']
#upgrades 50 databases in parallel to speedup migration


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
  
  #for sync_status in executor.map(parallel_sync, databases):
  #  sync_status_df = spark.createDataFrame(pd.read_json(sync_status, orient='columns'))
  #  syncresultsDF = unionAll(syncresultsDF, sync_status_df)
  #  executor.close()
  #  executor.join()

    #syncresultsDF = unionAll(syncresultsDF, sync_status)
#with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
  #deque(executor.map(parallel_sync, databases))

# COMMAND ----------

display(syncresultsDF)
