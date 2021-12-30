// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark SQL
// MAGIC 1. Run a SQL query
// MAGIC 1. Create DataFrame from table
// MAGIC 1. Write same query using DataFrame transformations
// MAGIC 1. Trigger computation with DataFrame actions
// MAGIC 1. Convert between DataFrames and SQL
// MAGIC 
// MAGIC ##### Methods
// MAGIC - SparkSession (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#spark-session-apis" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession.html" target="_blank">Scala</a>): `sql`, `table`
// MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>):
// MAGIC   - Transformations:  `select`, `where`, `orderBy`
// MAGIC   - Actions: `show`, `count`, `take`
// MAGIC   - Other methods: `printSchema`, `schema`, `createOrReplaceTempView`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup-SQL

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Run a SQL query
// MAGIC Use `SparkSession` to run SQL

// COMMAND ----------

val budgetDF = spark.sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

// COMMAND ----------

// MAGIC %md
// MAGIC View results in the returned DataFrame

// COMMAND ----------

budgetDF.show()

// COMMAND ----------

display(budgetDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create a DataFrame
// MAGIC Use `SparkSession` to create a DataFrame from a table

// COMMAND ----------

val productsDF = spark.table("products")
display(productsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Access schema of DataFrame

// COMMAND ----------

productsDF.printSchema()

// COMMAND ----------

productsDF.schema

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Write same query with DataFrame transformations

// COMMAND ----------

val budgetDF = (productsDF
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Trigger computation with DataFrame actions

// COMMAND ----------

budgetDF.count()

// COMMAND ----------

budgetDF.take(2)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Convert between DataFrames and SQL

// COMMAND ----------

budgetDF.createOrReplaceTempView("budget")

// COMMAND ----------

display(spark.sql("SELECT * FROM budget"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Spark SQL Lab
// MAGIC 
// MAGIC 1. Create a DataFrame from the `Event` table
// MAGIC 1. Display DataFrame and inspect schema
// MAGIC 1. Apply transformations to filter and sort `macOS` events
// MAGIC 1. Count results and take first 5 rows
// MAGIC 1. Create the same DataFrame using SQL query
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession.html" target="_blank">SparkSession</a>: `sql`, `table`
// MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">DataFrame</a> transformations: `select`, `where`, `orderBy`
// MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">DataFrame</a> actions: `select`, `count`, `take`
// MAGIC - Other <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">DataFrame</a> methods: `printSchema`, `schema`, `createOrReplaceTempView`

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create a DataFrame from the `events` table
// MAGIC - Use SparkSession to create a DataFrame from the `events` table

// COMMAND ----------

// ANSWER
val eventsDF = spark.table("events")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Display DataFrame and inspect schema
// MAGIC - Use methods above to inspect DataFrame contents and schema

// COMMAND ----------

// ANSWER
display(eventsDF)

// COMMAND ----------

// ANSWER
eventsDF.printSchema()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### 3. Apply transformations to filter and sort `macOS` events
// MAGIC - Filter for rows where `device` is `macOS`
// MAGIC - Sort rows by `event_timestamp`
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use single and double quotes in your filter SQL expression

// COMMAND ----------

// ANSWER
val macDF = eventsDF
  .where("device == 'macOS'")
  .sort("event_timestamp")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Count results and take first 5 rows
// MAGIC - Use DataFrame actions to count and take rows

// COMMAND ----------

// ANSWER
val numRows = macDF.count()
val rows = macDF.take(5)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

import org.apache.spark.sql.Row

assert(numRows == 1938215)
assert(rows.size == 5)
assert(rows(0).isInstanceOf[Row])

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Create the same DataFrame using SQL query
// MAGIC - Use SparkSession to run a sql query on the `events` table
// MAGIC - Use SQL commands above to write the same filter and sort query used earlier

// COMMAND ----------

// ANSWER
val macSQLDF = spark.sql("""
SELECT *
FROM events
WHERE device = 'macOS'
ORDER By event_timestamp
""")

display(macSQLDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC %md ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work
// MAGIC - You should only see `macOS` values in the `device` column
// MAGIC - The fifth row should be an event with timestamp `1592539226602157`

// COMMAND ----------

// MAGIC %md
// MAGIC ### Classroom Cleanup

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
