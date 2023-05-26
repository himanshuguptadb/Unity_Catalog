# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Use this notebook to apply ACLs on  database and table level grants from hive_external metastore  to UC upgraded DBs and Tables
# MAGIC
# MAGIC Prerequesites -  
# MAGIC 1. Create a shared comnpute with spark config **spark.databricks.acl.sqlOnly true**.   
# MAGIC 1. Ensure external metasore objects are accessible
# MAGIC 1. **READ_METADATA** previlige of legacy  hive metastore not available in UC yet which will be avaiable in coming releases

# COMMAND ----------

dbutils.widgets.text("tableToStoreApplyAcls", "srikanth_anumula_catalog.srikanth_uc_migrate.uc_table_acls_grants")
dbutils.widgets.text("tableToReadHmsAcls", "srikanth_anumula_catalog.srikanth_uc_migrate.table_acls")

#Table that stores output after applying ACLs
tableToStoreApplyAcls = dbutils.widgets.get("tableToStoreApplyAcls")
#Table that has legacy ACLs from Hive metastore
tableToReadHmsAcls = dbutils.widgets.get("tableToReadHmsAcls")

print(tableToStoreApplyAcls)
print(tableToReadHmsAcls)

# COMMAND ----------

#CatelogName to apply grants
dbutils.widgets.text("catalogName", "srikanth_anumula_catalog")
catalogName = dbutils.widgets.get("catalogName")

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col, lit

# COMMAND ----------

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code to apply grants

# COMMAND ----------

#function to apply grants at db level. There are instances where grants at just db level and applies to all objects within
def apply_database_grants(objectName):
    print(f'Applying grants for object: {objectName}')
    grants_columns = ["Principal","ActionType","ObjectType","ObjectName"]
    grantaclsSchema = StructType([
                        StructField('Principal', StringType(), True),
                        StructField('ActionType', StringType(), True),
                        StructField('ObjectType', StringType(), True),
                        StructField('ObjectName', StringType(), True),
                        StructField('Error', StringType(), True)
                        ])
    final_grants_df = spark.createDataFrame([], schema = grantaclsSchema)
    for grants in spark.sql(f"select Principal,ActionType,ObjectType,ObjectName from db_grantresultsDF where objectname = '{objectName}' ").collect():
      error = ''
      principal = grants['Principal']
      #OWN is not available in UC
      if grants['ActionType'] == 'OWN':
        actionType = 'ALL PRIVILEGES'
      elif grants['ActionType'] == 'CREATE_NAMED_FUNCTION':
        actionType = 'CREATE FUNCTION'
      else:
        actionType = grants['ActionType']

      try:
        ##Applying USE CATALOG on the principal to access Database and Table
        spark.sql(f"GRANT USE CATALOG ON CATALOG `{catalogName}` TO `{principal}`")
        if grants['ObjectType'] == 'DATABASE':
          spark.sql(f"GRANT {actionType} ON SCHEMA `{catalogName}`.{objectName} TO `{principal}` ")
        else:
          spark.sql(f"GRANT {actionType} ON TABLE `{catalogName}`.{objectName} TO `{principal}` ")
      except Exception as e:
        error = str(e)
      
      grants_df = spark.createDataFrame(data=[(principal,actionType,grants['ObjectType'],objectName)], schema = grants_columns)
      final_grants_df = unionAll(final_grants_df, grants_df.withColumn('Error', lit(error)))


    return final_grants_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Code reads legacy ACLs and calls above method to apply ACLs on UC tables

# COMMAND ----------

#Read from table which contains Hive metastore acls
spark.table(tableToReadHmsAcls).createOrReplaceTempView("db_grantresultsDF")

#Add TABLE to filter condition if we want to apply at Table level also
objects_to_apply = [row['objectname'] for row in spark.sql(f"select distinct objectname from  db_grantresultsDF where objecttype in ('DATABASE')").collect()]

grantaclsSchema = StructType([
  StructField('Principal', StringType(), True),
  StructField('ActionType', StringType(), True),
  StructField('ObjectType', StringType(), True),
  StructField('ObjectName', StringType(), True),
  StructField('Error', StringType(), True)
  ])

grantresultsDF = spark.createDataFrame([], schema = grantaclsSchema)

##Apply grants at object level for 50 in parallel to speedup migration
with ThreadPoolExecutor(max_workers=50) as executor:
    for object_grants in executor.map(apply_database_grants, objects_to_apply):
        grantresultsDF = unionAll(grantresultsDF, object_grants)

grantresultsDF.write.mode("overwrite").saveAsTable(tableToStoreApplyAcls)
#display(grantresultsDF)

# COMMAND ----------

# catalogName = 'srikanth_anumula_catalog'
# grants_columns = ["Principal","ActionType","ObjectType","ObjectName"]
# grantaclsSchema = StructType([
#                     StructField('Principal', StringType(), True),
#                     StructField('ActionType', StringType(), True),
#                     StructField('ObjectType', StringType(), True),
#                     StructField('ObjectName', StringType(), True),
#                     StructField('Error', StringType(), True)
#                     ])
# final_grants_df = spark.createDataFrame([], schema = grantaclsSchema)

# for grants in spark.sql(f"select Principal,ActionType,ObjectType,ObjectName from db_grantresultsDF where objectname = '{objectName}' ").collect():
#   error = ''
#   principal = grants['Principal']
#   #OWN is not available in UC
#   if grants['ActionType'] == 'OWN':
#     actionType = 'ALL PRIVILEGES'
#   else:
#     actionType = grants['ActionType']

#   try:
#     if grants['ObjectType'] == 'DATABASE':
#       spark.sql(f"GRANT {actionType} ON SCHEMA `{catalogName}`.{objectName} TO `{principal}` ")
#     else:
#       spark.sql(f"GRANT {actionType} ON TABLE `{catalogName}`.{objectName} TO `{principal}` ")
#   except Exception as e:
#     error = str(e)
  
#   grants_df = spark.createDataFrame(data=[(principal,actionType,grants['ObjectType'],objectName)], schema = grants_columns)
#   final_grants_df = unionAll(final_grants_df, grants_df.withColumn('Error', lit(error)))

# display(final_grants_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC show grants on catalog srikanth_anumula_catalog
