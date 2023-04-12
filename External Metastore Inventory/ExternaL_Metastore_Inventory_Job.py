# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook to capture external metastore inventory (except ACLs) to plan migration
# MAGIC 
# MAGIC Prerequesites -  
# MAGIC 1. Create a single node comnpute ensuring external metasore objects are accessible

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Definitions

# COMMAND ----------

def parallel_sync(database_name):
  print(database_name)
  return dbutils.notebook.run("ExternaL_Metastore_Inventory_Task", 60000, {"database": database_name })

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Script

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col, lit
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime


print('Getting table/view details from all dbs in external metastore')

#uncomment bellow line to run for all databases
#databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
#databases = databases[:500]

#Comment below line to run for all databases
databases = ['himanshu_gupta_demo_hms','000_demo_db','_fivetran_staging']

all_table_details_Columns = StructType([
  StructField('database_name', StringType(), True),
  StructField('table_name', StringType(), True),
  StructField('table_type', StringType(), True),
  StructField('storage_format', StringType(), True),
  StructField('managed', StringType(), True),
  StructField('storage_location', StringType(), True),
  #StructField('should_copy', StringType(), True),
  StructField('error', StringType(), True)
  ])
all_table_detailsDF = spark.createDataFrame(data=[], schema = all_table_details_Columns)
#upgrades 3 databases in parallel to speedup migration

with ThreadPoolExecutor(max_workers=100) as executor:
  futures = [
        executor.submit(parallel_sync, database) for database in databases
    ]
  
  for future in futures:
        print([f._state for f in futures])
        if future.cancelled():
            continue
        try:
            table_details = future.result()
            table_details_df = spark.createDataFrame(pd.read_json(table_details, orient='columns'))
            all_table_detailsDF = unionAll(all_table_detailsDF, table_details_df)
        except ValueError as e:
            print(f"{datetime.now()} - EXCEPTION! {e}")
            executor.shutdown(wait=False, cancel_futures=True)

print(f"{datetime.now()} - Run complete")

# COMMAND ----------

all_table_detailsDF.display()
