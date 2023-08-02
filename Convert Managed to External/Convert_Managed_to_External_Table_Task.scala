// Databricks notebook source
dbutils.widgets.text("dbName", "", "")
dbutils.widgets.text("tableName", "", "")

val dbName =  dbutils.widgets.get("dbName")
val tableName = dbutils.widgets.get("tableName")

// COMMAND ----------

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.TableIdentifier

// COMMAND ----------

println(s"Started conversion for dbName: $dbName, tableName: $tableName")
val oldTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(dbName)))
val alteredTable: CatalogTable = oldTable.copy(tableType = CatalogTableType.EXTERNAL)
val finalOutput = spark.sessionState.catalog.alterTable(alteredTable)
println(s"Conversion completed for $dbName and $tableName")

