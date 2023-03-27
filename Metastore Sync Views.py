# Databricks notebook source
# MAGIC %md
# MAGIC ## Upgrading views from external metastore to UC

# COMMAND ----------

def upgrade_database_views_advanced(database_to_upgrade, catalog_destination, database_destination = None, databases_upgraded = None,
                                    continue_on_exception = False, owner_to = None, privilege = None, privilege_principal = None):
  if database_destination == None:
    database_destination = database_to_upgrade
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
        print(f"creating view {table_name} as {view_definition}")
        spark.sql(f"CREATE OR REPLACE VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` AS {view_definition}")
        if owner_to is not None:
          spark.sql(f'ALTER VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` OWNER TO `{owner_to}`')
        if privilege is not None and "SELECT" in privilege or "ALL PRIVILEGES" in privilege:
          spark.sql(f'GRANT SELECT ON VIEW `{catalog_destination}`.`{database_destination}`.`{table_name}` TO `{privilege_principal}`');
      except Exception as e:
        if continue_on_exception:
          print(f"ERROR UPGRADING VIEW`{database_destination}`.`{table_name}`: {str(e)}. Continue")
        else:
          raise e


# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from collections import deque
from pyspark.sql.types import StructType,StructField, StringType


print('Sync all external databases to the UC:')
#databases = [row['databaseName'] for row in spark.sql(f"SHOW DATABASES IN hive_metastore").collect()]
databases = ['himanshu_gupta_demo_hms']
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
    for sync_status in executor.map(parallel_sync, databases):
      syncresultsDF = unionAll(syncresultsDF, sync_status)
