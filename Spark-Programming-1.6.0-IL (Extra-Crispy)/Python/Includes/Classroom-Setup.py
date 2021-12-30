# Databricks notebook source

module_name = "spark-programming"
spark.conf.set("com.databricks.training.module-name", module_name)

salesPath = "/mnt/training/ecommerce/sales/sales.parquet"
usersPath = "/mnt/training/ecommerce/users/users.parquet"
eventsPath = "/mnt/training/ecommerce/events/events.parquet"
productsPath = "/mnt/training/ecommerce/products/products.parquet"

# COMMAND ----------

# MAGIC %run ./Common-Notebooks/Common
