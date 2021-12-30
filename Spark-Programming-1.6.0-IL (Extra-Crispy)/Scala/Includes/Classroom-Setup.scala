// Databricks notebook source

val module_name = "spark-programming"
spark.conf.set("com.databricks.training.module-name", module_name)

val salesPath = "/mnt/training/ecommerce/sales/sales.parquet"
val usersPath = "/mnt/training/ecommerce/users/users.parquet"
val eventsPath = "/mnt/training/ecommerce/events/events.parquet"
val productsPath = "/mnt/training/ecommerce/products/products.parquet"

displayHTML("")

// COMMAND ----------

// MAGIC %run ./Common-Notebooks/Common
