// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # User-Defined Functions
// MAGIC 1. Define a function
// MAGIC 1. Create and apply UDF
// MAGIC 1. Register UDF to use in SQL
// MAGIC 1. Use Decorator Syntax (Python Only)
// MAGIC 1. Use Vectorized UDF (Python Only)
// MAGIC 
// MAGIC ##### Methods
// MAGIC - UDF Registration (`spark.udf`) (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.udf.html?#pyspark.sql.functions.udf" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/UDFRegistration.html" target="_blank">Scala</a>): `register`
// MAGIC - Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>): `udf`
// MAGIC - Python UDF Decorator (<a href="https://docs.databricks.com/spark/latest/spark-sql/udf-python.html#use-udf-with-dataframes" target="_blank">Databricks</a>): `@udf`
// MAGIC - Pandas UDF Decorator (<a href="https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html#pandas-user-defined-functions" target="_blank">Databricks</a>): `@pandas_udf`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

val salesDF = spark.read.parquet(salesPath)
display(salesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Define a function
// MAGIC 
// MAGIC Define a function in local Python/Scala to get the first letter of a string from the `email` field.

// COMMAND ----------

def firstLetterFunction (email: String): String = {
  email(0).toString
}

firstLetterFunction("annagray@kaufman.com")

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create and apply UDF
// MAGIC Define a UDF that wraps the function. This serializes the function and sends it to executors to be able to use in our DataFrame.

// COMMAND ----------

val firstLetterUDF = udf(firstLetterFunction _)

// COMMAND ----------

// MAGIC %md
// MAGIC Apply UDF on the `email` column.

// COMMAND ----------

import org.apache.spark.sql.functions.col
display(salesDF.select(firstLetterUDF(col("email"))))

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Register UDF to use in SQL
// MAGIC Register UDF using `spark.udf.register` to create UDF in the SQL namespace.

// COMMAND ----------

salesDF.createOrReplaceTempView("sales")

spark.udf.register("sql_udf", firstLetterFunction _)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sql_udf(email) AS firstLetter FROM sales

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Use Decorator Syntax (Python Only)
// MAGIC 
// MAGIC Alternatively, define UDF using decorator syntax in Python with the datatype the function returns.
// MAGIC 
// MAGIC You will no longer be able to call the local Python function (e.g. `decoratorUDF("annagray@kaufman.com")` will not work)

// COMMAND ----------

// MAGIC %python
// MAGIC # Our input/output is a string
// MAGIC @udf("string")
// MAGIC def decoratorUDF(email: str) -> str:
// MAGIC   return email[0]

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC salesDF = spark.read.parquet("/mnt/training/ecommerce/sales/sales.parquet")
// MAGIC display(salesDF.select(decoratorUDF(col("email"))))

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Use Vectorized UDF (Python Only)
// MAGIC 
// MAGIC Use Vectorized UDF to help speed up the computation using Apache Arrow.

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC from pyspark.sql.functions import pandas_udf
// MAGIC 
// MAGIC # We have a string input/output
// MAGIC @pandas_udf("string")
// MAGIC def vectorizedUDF(email: pd.Series) -> pd.Series:
// MAGIC   return email.str[0]
// MAGIC 
// MAGIC # Alternatively
// MAGIC vectorizedUDF = pandas_udf(lambda s: s.str[0], "string")

// COMMAND ----------

// MAGIC %python
// MAGIC display(salesDF.select(vectorizedUDF(col("email"))))

// COMMAND ----------

// MAGIC %md
// MAGIC We can also register these Vectorized UDFs to the SQL namespace.

// COMMAND ----------

// MAGIC %python
// MAGIC spark.udf.register("sql_vectorized_udf", vectorizedUDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Sort Day Lab
// MAGIC 1. Define UDF to label day of week
// MAGIC 1. Apply UDF to label and sort by day of week
// MAGIC 1. Plot active users by day of week as bar graph

// COMMAND ----------

// MAGIC %md
// MAGIC Start with a DataFrame of the average number of active users by day of week.
// MAGIC 
// MAGIC This was the resulting `df` in a previous lab.

// COMMAND ----------

import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, date_format, to_date}

val df = spark.read.parquet(eventsPath)
  .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
  .withColumn("date", to_date(col("ts")))
  .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
  .withColumn("day", date_format(col("date"), "E"))
  .groupBy("day").agg(avg(col("active_users")).alias("avg_users"))

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Define UDF to label day of week
// MAGIC - Use the **`labelDayOfWeek`** provided below to create the udf **`labelDowUDF`**

// COMMAND ----------

def labelDayOfWeek(day:String): String = {
  day match {
    case "Mon" => "1-Mon"
    case "Tue" => "2-Tue"
    case "Wed" => "3-Wed"
    case "Thu" => "4-Thu"
    case "Fri" => "5-Fri"
    case "Sat" => "6-Sat"
    case "Sun" => "7-Sun"
    case _ => "UNKNOWN"
  }
}

// COMMAND ----------

// TODO
val labelDowUDF = FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Apply UDF to label and sort by day of week
// MAGIC - Update the **`day`** column by applying the UDF and replacing this column
// MAGIC - Sort by **`day`**
// MAGIC - Plot as bar graph

// COMMAND ----------

// TODO
val finalDF = FILL_IN

display(finalDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
