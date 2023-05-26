# Databricks notebook source
dbutils.widgets.text("Source Catalog", "", "")
dbutils.widgets.text("Source Databases", "", "")
dbutils.widgets.text("Target Catalog", "", "")
dbutils.widgets.text("Target Databases", "", "")
dbutils.widgets.text("Service Account", "", "")
dbutils.widgets.dropdown("Dry Run","Yes", ["No", "Yes"])

# COMMAND ----------

import ast
import pandas as pd
from pyspark.sql.functions import when

source_catalog =  dbutils.widgets.get("Source Catalog")
source_database =  dbutils.widgets.get("Source Databases").split(",")
target_catalog =  dbutils.widgets.get("Target Catalog")
target_database =  dbutils.widgets.get("Target Databases").split(",")
service_account =  dbutils.widgets.get("Service Account")
run =  dbutils.widgets.get("Dry Run")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace
def metadata_query(catalog,database):
  
  """
  Args:
      database (str):          database source to upgrade (in hive_metastore)
  """      
  print(f'Querying database under `{catalog}`.`{database}`.')
  #Now iterate over all the tables or views with the database
  table_details_Columns = StructType([
  StructField('database', StringType(), True),
  StructField('tableName', StringType(), True),
  StructField('Location', StringType(), True),
  StructField('Type', StringType(), True)
  ])
  db_table_details = spark.createDataFrame([], schema = table_details_Columns)
  spark.sql(f"use catalog {catalog}")
  metadata = spark.sql(f"SHOW TABLE EXTENDED  IN {catalog}.{database} LIKE '*'")
  spark.sql("set spark.sql.mapKeyDedupPolicy = LAST_WIN")

  keys = ["Location","Type"]
  error = ""

  metadata = metadata.withColumn('information_detail', F.expr("str_to_map(information,'\n',':')"))\
                            .selectExpr("*", *[ f"information_detail['{k}'] as `{k}`" for k in keys ])\
                            .drop("information")\
                            .drop("information_detail")\
                            .drop("isTemporary")\
                            .withColumn('Location', F.expr("regexp_replace(Location, tableName, '')"))\
                            .filter(col("Type").like('%EXTERNAL%'))\
                            .drop("Type","database","tableName")\
                            .dropDuplicates(["Location"])\

  metadata_parsed = metadata.select([col(c).cast("string") for c in metadata.columns])  

  return metadata_parsed

# COMMAND ----------

grants = []
source_catalog_grant = "GRANT USE_CATALOG ON CATALOG " + source_catalog + " TO " + service_account + " ;"
grants.append(source_catalog_grant)
target_catalog_grant = "GRANT USE_CATALOG ON CATALOG " + target_catalog + " TO " + service_account + " ;"
grants.append(target_catalog_grant)

for database in source_database:
  database_use_grant = "GRANT USE_SCHEMA ON SCHEMA " + source_catalog +"."+database + " TO " + service_account + " ;"
  database_select_grant = "GRANT SELECT ON SCHEMA " + source_catalog +"."+database + " TO " + service_account + " ;"
  grants.append(database_use_grant)
  grants.append(database_select_grant)

external_locations = spark.sql("SHOW EXTERNAL LOCATIONS")

for database in target_database:
  database_use_grant = "GRANT USE_SCHEMA ON SCHEMA " + target_catalog +"."+database + " TO " + service_account + " ;"
  database_select_grant = "GRANT SELECT ON SCHEMA " + target_catalog +"."+database + " TO " + service_account + " ;"
  database_create_table_grant = "GRANT CREATE TABLE ON SCHEMA " + target_catalog +"."+database + " TO " + service_account + " ;"
  database_modify_grant = "GRANT MODIFY ON SCHEMA " + target_catalog +"."+database + " TO " + service_account + " ;"
  grants.append(database_use_grant)
  grants.append(database_select_grant)
  grants.append(database_create_table_grant)
  grants.append(database_modify_grant)

  database_external_locations = metadata_query(target_catalog,database)

  database_external_locations = database_external_locations.crossJoin(external_locations)
  external_location_to_grant = b.withColumn("Matched", when(b.Location.contains(b.url),"1")
                                                      .when(b.url.contains(b.Location),"1")
                                                      .otherwise("0"))\
                                .filter(col("Matched") == "1")\
                                .select("name")\
                                .collect()
  print(external_location_to_grant)
  for row in external_location_to_grant:
    external_location = row['name']
    display(row['name'])
    external_location_grant = "GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION " + external_location + " TO " +  service_account + " ;"
    grants.append(external_location_grant)


# COMMAND ----------

grants
