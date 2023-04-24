# Databricks notebook source
# MAGIC %md
# MAGIC ## Upgrading views from external metastore to UC

# COMMAND ----------

def upgrade_database_views(database_to_upgrade, catalog_destination, database_destination = None, databases_upgraded = None,
                                    database_owner_to = None, privilege = None, privilege_principal = None):
  if database_destination == None:
    database_destination = database_to_upgrade
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
    
  sync_statusDF = spark.createDataFrame([], schema = syncColumns)
    
  for row in spark.sql(f"SHOW TABLES IN hive_metastore.{database_to_upgrade}").collect():
    table_name = row['tableName']
    full_table_name_source = f'`hive_metastore`.`{database_to_upgrade}`.`{table_name}`'
    full_table_name_destination = f'`{catalog_destination}`.`{database_destination}`.`{table_name}`'
    properties = spark.sql(f"describe extended {full_table_name_source}").where("col_name = 'View Text'").collect()
    if len(properties) > 0:
      try:
        view_definition = properties[0]['data_type']
        #Try to replace all view definition with the one being merged on the new catalog
        view_definition = re.sub(rf"(`?hive_metastore`?\.`?{database_to_upgrade}`?)", f"`{catalog_destination}`.`{database_destination}`", view_definition)
        for db_source, db_destibation in databases_upgraded:
          view_definition = re.sub(rf"(`?hive_metastore`?\.`?{db_source}`?)", f"`{catalog_destination}`.`{db_destibation}`", view_definition)
        spark.sql(f"CREATE OR REPLACE VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` AS {view_definition}")
        if database_owner_to is not None:
          spark.sql(f'ALTER VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` OWNER TO `{database_owner_to}`')
        if privilege is not None and "SELECT" in privilege or "ALL PRIVILEGES" in privilege:
          spark.sql(f'GRANT SELECT ON VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` TO `{privilege_principal}`');
          
        view_sync_statusDF = spark.createDataFrame(data=[(database_to_upgrade,table_name,"VIEW",catalog_destination,database_destination,table_name,"SUCCESS","VIEW CREATED SUCESSFULLY")], schema = syncColumns)
        sync_statusDF = unionAll(sync_statusDF, view_sync_statusDF)
      except Exception as e:
        error = str(e)
        view_error_sync_statusDF = []
        view_error_sync_statusDF = spark.createDataFrame(data=[(database_to_upgrade,table_name,"VIEW",catalog_destination,database_destination,table_name,"Error",error)], schema = syncColumns)
        sync_statusDF = unionAll(sync_statusDF, view_error_sync_statusDF)
        display(sync_statusDF)
        print(f"ERROR UPGRADING VIEW`{database_destination}`.`{table_name}`: {str(e)}. Continue")
        
  return sync_statusDF

def parallel_sync(database_name):
  
  return upgrade_database_views(database_to_upgrade = database_name, catalog_destination = "himanshu_gupta_demos", database_destination = database_name, databases_upgraded = databases_upgraded, database_owner_to = 'account users')

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType
import re
from pyspark.sql import DataFrame

print('Sync all external databases to the UC:')
#databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
databases = ['himanshu_gupta_demo_hms']
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
    for sync_status in executor.map(parallel_sync, databases):
      syncresultsDF = unionAll(syncresultsDF, sync_status)
