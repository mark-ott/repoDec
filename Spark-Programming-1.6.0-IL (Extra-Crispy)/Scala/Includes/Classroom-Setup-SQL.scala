// Databricks notebook source
// MAGIC 
// MAGIC %run ./Classroom-Setup

// COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "%s")""".format(eventsPath))
spark.sql("""CREATE TABLE IF NOT EXISTS sales USING parquet OPTIONS (path "%s")""".format(salesPath))
spark.sql("""CREATE TABLE IF NOT EXISTS users USING parquet OPTIONS (path "%s")""".format(usersPath))
spark.sql("""CREATE TABLE IF NOT EXISTS products USING parquet OPTIONS (path "%s")""".format(productsPath))

displayHTML("")

