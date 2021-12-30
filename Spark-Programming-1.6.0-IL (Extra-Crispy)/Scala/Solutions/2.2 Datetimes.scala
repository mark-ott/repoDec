// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Datetime Functions
// MAGIC 1. Cast to timestamp
// MAGIC 2. Format datetimes
// MAGIC 3. Extract from timestamp
// MAGIC 4. Convert to date
// MAGIC 5. Manipulate datetimes
// MAGIC 
// MAGIC ##### Methods
// MAGIC - Column (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html#pyspark.sql.Column" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html" target="_blank">Scala</a>): `cast`
// MAGIC - Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>): `date_format`, `to_date`, `date_add`, `year`, `month`, `dayofweek`, `minute`, `second`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use a subset of the BedBricks events dataset to practice working with date times.

// COMMAND ----------

import org.apache.spark.sql.functions.col

val df = spark.read.parquet(eventsPath).select(col("user_id"), col("event_timestamp").alias("timestamp"))
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Cast to Timestamp

// COMMAND ----------

// MAGIC %md
// MAGIC #### `cast()`
// MAGIC Casts column to a different data type, specified using string representation or DataType.

// COMMAND ----------

val timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestampDF)

// COMMAND ----------

import org.apache.spark.sql.types.TimestampType

val timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType))
display(timestampDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Format date

// COMMAND ----------

// MAGIC %md
// MAGIC #### `date_format()`
// MAGIC Converts a date/timestamp/string to a string formatted with the given date time pattern.
// MAGIC 
// MAGIC See valid date and time format patterns for <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">Spark 3</a> and <a href="https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html" target="_blank">Spark 2</a>.

// COMMAND ----------

import org.apache.spark.sql.functions.date_format

val formattedDF = timestampDF.withColumn("date string", date_format(col("timestamp"), "MMMM dd, yyyy"))
  .withColumn("time string", date_format(col("timestamp"), "HH:mm:ss.SSSSSS"))

display(formattedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Extract datetime attribute from timestamp

// COMMAND ----------

// MAGIC %md
// MAGIC #### `year`
// MAGIC Extracts the year as an integer from a given date/timestamp/string.
// MAGIC 
// MAGIC ##### Similar methods: `month`, `dayofweek`, `minute`, `second`, etc.

// COMMAND ----------

import org.apache.spark.sql.functions.{year, month, dayofweek, minute, second}

val datetimeDF = timestampDF.withColumn("year", year(col("timestamp")))
  .withColumn("month", month(col("timestamp")))
  .withColumn("dayofweek", dayofweek(col("timestamp")))
  .withColumn("minute", minute(col("timestamp")))
  .withColumn("second", second(col("timestamp")))

display(datetimeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Convert to Date

// COMMAND ----------

// MAGIC %md
// MAGIC #### `to_date`
// MAGIC Converts the column into DateType by casting rules to DateType.

// COMMAND ----------

import org.apache.spark.sql.functions.to_date

val dateDF = timestampDF.withColumn("date", to_date(col("timestamp")))
display(dateDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Manipulate Datetimes

// COMMAND ----------

// MAGIC %md
// MAGIC #### `date_add`
// MAGIC Returns the date that is the given number of days after start

// COMMAND ----------

import org.apache.spark.sql.functions.date_add

val plus2DF = timestampDF.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus2DF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Active Users Lab
// MAGIC Plot daily active users and average active users by day of week.
// MAGIC 1. Extract timestamp and date of events
// MAGIC 2. Get daily active users
// MAGIC 3. Get average number of active users by day of week
// MAGIC 4. Sort day of week in correct order

// COMMAND ----------

// MAGIC %md
// MAGIC ### Setup
// MAGIC Run the cell below to create the starting DataFrame of user IDs and timestamps of events logged on the BedBricks website.

// COMMAND ----------

val df = spark.read.parquet(eventsPath)
  .select(col("user_id"), col("event_timestamp").alias("ts"))

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Extract timestamp and date of events
// MAGIC - Convert **`ts`** from microseconds to seconds by dividing by 1 million and cast to timestamp
// MAGIC - Add **`date`** column by converting **`ts`** to date

// COMMAND ----------

// ANSWER
val datetimeDF = df.withColumn("ts", (col("ts") / 1e6).cast("timestamp"))
  .withColumn("date", to_date(col("ts")))

display(datetimeDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}

val expected1a = StructType(List(StructField("user_id", StringType, true),
  StructField("ts", TimestampType, true),
  StructField("date", DateType, true)))

val result1a = datetimeDF.schema

assert(expected1a == result1a, "datetimeDF does not have the expected schema")

// COMMAND ----------

val expected1b = "2020-06-19"
val result1b = datetimeDF.sort("date").first().get(2).toString

assert(expected1b.sameElements(result1b), "datetimeDF does not have the expected date values")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Get daily active users
// MAGIC - Group by date
// MAGIC - Aggregate approximate count of distinct **`user_id`** and alias with "active_users"
// MAGIC   - Recall built-in function to get approximate count distinct
// MAGIC - Sort by date
// MAGIC - Plot as line graph

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.approx_count_distinct

val activeUsersDF = datetimeDF.groupBy("date")
    .agg(approx_count_distinct("user_id").alias("active_users"))
  .sort("date")

display(activeUsersDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

import org.apache.spark.sql.types.LongType

val expected2a = StructType(List(StructField("date", DateType, true),
  StructField("active_users", LongType, false)))

val result2a = activeUsersDF.schema

assert(expected2a == result2a, "activeUsersDF does not have the expected schema")

// COMMAND ----------

val expected2b = Array(("2020-06-19",251573), ("2020-06-20",357215), ("2020-06-21",305055), ("2020-06-22",239094), ("2020-06-23",243117))

val result2b = for (row <- activeUsersDF.take(5)) yield (row.get(0).toString, row.get(1))

assert(expected2b.sameElements(result2b), "activeUsersDF does not have the expected values")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Get average number of active users by day of week
// MAGIC - Add **`day`** column by extracting day of week from **`date`** using a datetime pattern string
// MAGIC - Group by **`day`**
// MAGIC - Aggregate average of **`active_users`** and alias with "avg_users"

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.{date_format, avg}

val activeDowDF = activeUsersDF.withColumn("day", date_format(col("date"), "E"))
  .groupBy("day")
    .agg(avg(col("active_users")).alias("avg_users"))

display(activeDowDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

import org.apache.spark.sql.types.DoubleType

val expected3a = StructType(List(StructField("day", StringType, true),
  StructField("avg_users", DoubleType, true)))

val result3a = activeDowDF.schema

assert(expected3a.sameElements(result3a), "activeDowDF does not have the expected schema")

// COMMAND ----------

val expected3b = Seq(("Fri", 247180.66666666666), ("Mon", 238195.5), ("Sat", 278482.0), ("Sun", 282905.5), ("Thu", 264620.0), ("Tue", 260942.5), ("Wed", 227214.0))

val result3b = for (row <- activeDowDF.sort("day").collect()) yield (row.get(0), row.get(1))

assert(expected3b.sameElements(result3b), "activeDowDF does not have the expected values")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
