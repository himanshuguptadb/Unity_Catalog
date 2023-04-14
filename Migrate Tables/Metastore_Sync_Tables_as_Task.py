# Databricks notebook source
# MAGIC %md
# MAGIC ## Sync sciprt for external tables

# COMMAND ----------

dbutils.widgets.text("database", "", "")
dbutils.widgets.text("catalog", "", "")
dbutils.widgets.text("owner", "", "")
dbutils.widgets.text("run", "", "")

database =  dbutils.widgets.get("database")
catalog =  dbutils.widgets.get("catalog")
owner =  dbutils.widgets.get("owner")
run =  dbutils.widgets.get("run")

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType,StructField, StringType

def sync_databases(database_to_upgrade, catalog_destination, database_destination = None, database_owner_to = None, dry_run = None):
  """Sync all tabes from one databse to the UC.
  Args:
      database_to_upgrade (str):          database source to upgrade (in hive_metastore)
      catalog_destination (str):          catalog destination (in unity catalog)
      database_destination (str):         name of the destibation database. If not defined will use the same as the source.
      database_owner_to (str):            Principal Owner of the database (default is None)
  """      
  #First create the new CATALOG in UC if it doesn't exist.
  spark.sql(f'CREATE CATALOG IF NOT EXISTS `{catalog_destination}`')
  #Then we create the database in the new UC catalog:
  if database_destination == None:
    database_destination = database_to_upgrade
  spark.sql(f'CREATE DATABASE IF NOT EXISTS `{catalog_destination}`.`{database_destination}`')
  
  print(f'Upgrading database under `hive_metastore`.`{database_to_upgrade}` to UC database `{catalog_destination}`.`{database_destination}`.')
  syncColumns = ["source_schema","source_name","source_type","target_catalog","target_schema","target_name","status_code","description"]
  syncColumns = StructType([
  StructField('source_schema', StringType(), True),
  StructField('source_name', StringType(), True),
  StructField('source_type', StringType(), True),
  StructField('target_catalog', StringType(), True),
  StructField('target_schema', StringType(), True),
  StructField('target_name', StringType(), True),
  StructField('status_code', StringType(), True),
  StructField('description', StringType(), True)
  ])
  
  if len(spark.sql(f"SHOW TABLES IN hive_metastore.{database_to_upgrade}").collect()) != 0:
    #Now iterate over all the table to synch them with the UC databse
    if dry_run == "True":
      sync_status = spark.sql(f"SYNC SCHEMA {catalog_destination}.{database_destination} FROM hive_metastore.{database_to_upgrade} SET OWNER `{database_owner_to}` DRY RUN").collect()
    else:
      sync_status = spark.sql(f"SYNC SCHEMA {catalog_destination}.{database_destination} FROM hive_metastore.{database_to_upgrade} SET OWNER `{database_owner_to}`").collect()
  
    sync_statusDF = spark.createDataFrame(data=sync_status, schema = syncColumns)
  else:
    sync_statusDF = spark.createDataFrame([(database_to_upgrade,"","",catalog_destination,database_destination,"","SUCCESS","Empty Database")], schema = syncColumns)
    
  #sync_statusDF.write.mode("append").format("parquet").saveAsTable("himanshu_gupta_demos.uc_upgrade.table_sync_status_p")
  return sync_statusDF
  

# COMMAND ----------

sync_status = sync_databases(database_to_upgrade = database, catalog_destination = catalog, database_destination = database, database_owner_to = owner, dry_run = run)
sync_status_j = sync_status.toPandas().to_json(orient='records')
dbutils.notebook.exit(sync_status_j)
