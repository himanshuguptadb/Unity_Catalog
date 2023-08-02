// Databricks notebook source
// MAGIC %md 
// MAGIC ### This script converts all the managed tables in a given table to external type 
// MAGIC Prerequised to run : 
// MAGIC 1. Run with cluster with 12.2
// MAGIC 2. Check val databases values

// COMMAND ----------

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.TableIdentifier

// COMMAND ----------


// Input your database names 
val databases = Array("mvxbdta100_raw_uctest") //Array("MVXBDTA100_RAW","MVXBDTA200_RAW","MVXBDTA250_RAW","MVXBDTA270_RAW","MVXBDTA370_RAW","MVXBDTA190_RAW")

for (dbName <- databases){
  val tableRows = spark.sql(s"SHOW TABLES in $dbName").collect()
  var tables = tableRows.map(_.getString(1))

  for(tableName <- tables){            
    dbutils.notebook.run("Convert_Managed_to_External_Table_Task", 10, Map("dbName" -> dbName, "tableName" -> tableName))
}
}



