# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Use this notebook to extract database and it's objects level grants from hive_external metastore  
# MAGIC
# MAGIC Prerequesites -  
# MAGIC 1. Use a non UC enabled workspace
# MAGIC 1. Create a shared comnpute with spark config **spark.databricks.acl.sqlOnly true**.   
# MAGIC 1. Ensure external metasore objects are accessible

# COMMAND ----------

dbutils.widgets.text("tableToStoreAcls", "srikanth_anumula_catalog.srikanth_uc_migrate.table_acls")
tableToStoreAcls = dbutils.widgets.get("tableToStoreAcls")
print(tableToStoreAcls)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code to retreive ACLs on Database and Tables from Hive Metastore

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

#function to extract grants at db level. There are instances where grants at just db level and applies to all objects within
def database_grants(database_name):
    """
    Args:
      database_name (str): database source for grants (in hive_metastore)
    """      

  
    print(f'Extracting grants from database under `hive_metastore`.`{database_name}`.')
    db_grants_columns = ["Principal","ActionType","ObjectType","ObjectName"]
    error = ''
    try:
        grants = spark.sql(f"Show grants on schema  hive_metastore.{database_name}").collect()
    except Exception as e:
        error = str(e)
        grants = []
        
   
    if grants == []:
        db_grants_df = spark.createDataFrame(data=[("","","DATABASE",database_name)], schema = db_grants_columns)
    else:
        db_grants_df = spark.createDataFrame(data=grants, schema = db_grants_columns)
    
    db_grants_df = db_grants_df.withColumn('Error', lit(error))
    return db_grants_df

#function to extract grants at table level. There are instances where grants at just table level. It will also give grants at db level. Which will result in some duplicates
def table_grants(database_name):
    
    table_grants_columns = ["Principal","ActionType","ObjectType","ObjectName"]
    table_grants_columns = StructType([
        StructField('Principal', StringType(), True),
        StructField('ActionType', StringType(), True),
        StructField('ObjectType', StringType(), True),
        StructField('ObjectName', StringType(), True)
        ])
    db_grants_df = spark.createDataFrame([], schema = table_grants_columns)
    error = ''

    try:
        for row in spark.sql(f"SHOW TABLES IN hive_metastore.{database_name}").collect():
            table_name = row['tableName']
            full_table_name = f'`hive_metastore`.`{database_name}`.`{table_name}`'
            grants = spark.sql(f"Show grants on table  {full_table_name}").collect()
            table_grants_df = spark.createDataFrame(data=grants, schema = table_grants_columns)
            db_grants_df = unionAll(db_grants_df, table_grants_df)
    except Exception as e:
        error = str(e)
        db_grants_df = []
  
    if db_grants_df == []:
        db_grants_df = spark.createDataFrame(data=[("","","TABLE",database_name)], schema = table_grants_columns)
    
    db_grants_df = db_grants_df.withColumn('Error', lit(error))
    return db_grants_df

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Main script to fetch ACLs to run in Parallel for all Databases in Hive Metastore

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col, lit


print('Extract grants from all databases in external metastore:')
#un comment below code if you want to run it for all the databases
databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
#comment below code to exit from test mode
databases = ['srikanth_demo']
#databases = databases[:10]



grantsultsSchema = StructType([
  StructField('Principal', StringType(), True),
  StructField('ActionType', StringType(), True),
  StructField('ObjectType', StringType(), True),
  StructField('ObjectName', StringType(), True),
  StructField('Error', StringType(), True)
  ])

db_grantresultsDF = spark.createDataFrame([], schema = grantsultsSchema)

tb_grantresultsDF = spark.createDataFrame([], schema = grantsultsSchema)

##Extracts grants at database level for 50 databases in parallel to speedup the process
with ThreadPoolExecutor(max_workers=50) as executor:
    for db_grants in executor.map(database_grants, databases):
        db_grantresultsDF = unionAll(db_grantresultsDF, db_grants)

##Extracts grants at table level for 50 databases in parallel to speedup the process
with ThreadPoolExecutor(max_workers=50) as executor:
    for db_grants in executor.map(table_grants, databases):
        tb_grantresultsDF = unionAll(tb_grantresultsDF, db_grants)
        
db_grantresultsDF = unionAll(db_grantresultsDF, tb_grantresultsDF)
db_grantresultsDF = db_grantresultsDF.dropDuplicates(subset=['Principal', 'ActionType', 'ObjectType', 'ObjectName', 'Error'])

db_grantresultsDF.write.mode("overwrite").saveAsTable(tableToStoreAcls)
#display(db_grantresultsDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from srikanth_anumula_catalog.srikanth_uc_migrate.table_acls

# COMMAND ----------

# MAGIC %sql
# MAGIC show grants on schema hive_metastore.srikanth_demo
