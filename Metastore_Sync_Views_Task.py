# Databricks notebook source
# MAGIC %md
# MAGIC ## Upgrading views from external metastore to UC

# COMMAND ----------

dbutils.widgets.text("database", "", "")
dbutils.widgets.text("catalog", "", "")
dbutils.widgets.text("owner", "", "")

database =  dbutils.widgets.get("database")
catalog =  dbutils.widgets.get("catalog")
owner =  dbutils.widgets.get("owner")

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType,StructField, StringType
import pandas as pd
import re

def upgrade_database_views(database_to_upgrade, catalog_destination, database_destination = None, databases_upgraded = None,
                                    database_owner_to = None, privilege = None, privilege_principal = None):
  
  databases_upgraded = spark.sql(f"select table_schema, table_name  from system.information_schema.tables where table_catalog != 'hive_metastore' and table_schema != 'information_schema'").collect()
  
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

def unionAll(*dfs):
	return reduce(DataFrame.unionAll, dfs)

# COMMAND ----------

sync_status = upgrade_database_views(database_to_upgrade = database, catalog_destination = catalog, database_destination = database, database_owner_to = owner)
sync_status_j = sync_status.toPandas().to_json(orient='records')
dbutils.notebook.exit(sync_status_j)
