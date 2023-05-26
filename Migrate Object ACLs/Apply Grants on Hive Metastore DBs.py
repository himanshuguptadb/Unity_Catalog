# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Use this notebook to apply ACLs on database and table level grants on hive metastore(LEGACY), notebook takes list of AD groups as paramter and apply corresponding previliges stored in the table
# MAGIC
# MAGIC Prerequesites -  
# MAGIC 1. Create a shared comnpute with spark config **spark.databricks.acl.sqlOnly true**.   
# MAGIC 1. Provide list of AD groups with comma(,) seperated

# COMMAND ----------

dbutils.widgets.text("tableToStoreApplyAcls", "srikanth_anumula_catalog.srikanth_uc_migrate.uc_table_acls_grants")
dbutils.widgets.text("tableToReadHmsAcls", "srikanth_anumula_catalog.srikanth_uc_migrate.table_acls")
dbutils.widgets.text("ADGroups", "srikanth.anumula@databricks.com,himanshu.gupta@databricks.com")

#Table that stores output after applying ACLs
tableToStoreApplyAcls = dbutils.widgets.get("tableToStoreApplyAcls")
#Table that has legacy ACLs from Hive metastore
tableToReadHmsAcls = dbutils.widgets.get("tableToReadHmsAcls")
#AD groups
ADGroups = dbutils.widgets.get("ADGroups").split(',')

print(tableToStoreApplyAcls)
print(tableToReadHmsAcls)
print(ADGroups)

# COMMAND ----------

#CatelogName to apply grants
dbutils.widgets.text("catalogName", "hive_metsatore")
catalogName = dbutils.widgets.get("catalogName")
print(catalogName)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

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
      actionType = grants['ActionType']

      try:
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
spark.table(tableToReadHmsAcls).filter(F.col("Principal").isin(ADGroups)).createOrReplaceTempView("db_grantresultsDF")

#Add TABLE to filter condition if we want to apply at Table level also
objects_to_apply = [row['objectname'] for row in spark.sql(f"select distinct objectname from  db_grantresultsDF where objecttype in ('DATABASE','TABLE')").collect()]

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

grantresultsDF.write.mode("append").saveAsTable(tableToStoreApplyAcls)
#display(grantresultsDF)
