// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Streaming Query
// MAGIC 1. Build streaming DataFrames
// MAGIC 1. Display streaming query results
// MAGIC 1. Write streaming query results
// MAGIC 1. Monitor streaming query
// MAGIC 
// MAGIC ##### Classes
// MAGIC - DataStreamReader (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/DataStreamReader.html" target="_blank">Scala</a>)
// MAGIC - DataStreamWriter (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/DataStreamWriter.html" target="_blank">Scala</a>)
// MAGIC - StreamingQuery (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/StreamingQuery.html" target="_blank">Scala</a>)

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

val schema = "device STRING, ecommerce STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT<city: STRING, state: STRING>, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING"

val df = spark.readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet(eventsPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Build streaming DataFrames

// COMMAND ----------

df.isStreaming

// COMMAND ----------

import org.apache.spark.sql.functions.{col, approx_count_distinct, count}

val emailTrafficDF = df.filter(col("traffic_source") === "email")
  .withColumn("mobile", col("device").isin("iOS", "Android"))
  .select("user_id", "event_timestamp", "mobile")

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Write streaming query results

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val checkpointPath = workingDir + "/email_traffic/checkpoint"
val outputPath = userhome + "/email_traffic/output"

val devicesQuery = emailTrafficDF.writeStream
  .outputMode("append")
  .format("parquet")
  .queryName("email_traffic_s")
  .trigger(Trigger.ProcessingTime("1 second"))
  .option("checkpointLocation", checkpointPath)
  .start(outputPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Monitor streaming query

// COMMAND ----------

devicesQuery.id

// COMMAND ----------

devicesQuery.status

// COMMAND ----------

devicesQuery.awaitTermination(5)

// COMMAND ----------

devicesQuery.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Coupon Sales Lab
// MAGIC Process and append streaming data on transactions using coupons.
// MAGIC 1. Read data stream
// MAGIC 2. Filter for transactions with coupons codes
// MAGIC 3. Write streaming query results to parquet
// MAGIC 4. Monitor streaming query
// MAGIC 5. Stop streaming query
// MAGIC 
// MAGIC ##### Classes
// MAGIC - [DataStreamReader](http://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/streaming/DataStreamReader.html)
// MAGIC - [DataStreamWriter](http://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/streaming/DataStreamWriter.html)
// MAGIC - [StreamingQuery](http://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/streaming/StreamingQuery.html)

// COMMAND ----------

val schema = "order_id BIGINT, email STRING, transaction_timestamp BIGINT, total_item_quantity BIGINT, purchase_revenue_in_usd DOUBLE, unique_items BIGINT, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>"

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Read data stream
// MAGIC - Use schema stored in **`schema`**
// MAGIC - Set to process 1 file per trigger
// MAGIC - Read from parquet with filepath stored in **`salesPath`**
// MAGIC 
// MAGIC Assign the resulting DataFrame to **`df`**

// COMMAND ----------

// ANSWER
val df = spark.readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet(salesPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(df.isStreaming)
assert(df.columns.sameElements(Array("order_id", "email", "transaction_timestamp", "total_item_quantity", "purchase_revenue_in_usd", "unique_items", "items")) )

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Filter for transactions with coupon codes
// MAGIC - Explode **`items`** field in **`df`**
// MAGIC - Filter for records where **`items.coupon`** is not null
// MAGIC 
// MAGIC Assign the resulting DataFrame to **`couponSalesDF`**.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.{col, explode}

val couponSalesDF = df.withColumn("items", explode(col("items")))
  .filter(col("items.coupon").isNotNull)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

val schemaStr = couponSalesDF.schema.mkString("")
assert(schemaStr.contains("StructField(items,StructType(StructField(coupon"), "items column was not exploded")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Write streaming query results to parquet
// MAGIC - Configure streaming query to write out to parquet in "append" mode
// MAGIC - Set query name to "coupon_sales"
// MAGIC - Set a trigger interval of 1 second
// MAGIC - Set checkpoint location to **`couponsCheckpointPath`**
// MAGIC - Set output filepath to **`couponsOutputPath`**
// MAGIC 
// MAGIC Assign the resulting streaming query to **`couponSalesQuery`**.

// COMMAND ----------

// ANSWER

val couponsCheckpointPath = workingDir + "/coupon-sales/checkpoint"
val couponsOutputPath = workingDir + "/coupon-sales/output"

val couponSalesQuery = couponSalesDF.writeStream
  .outputMode("append")
  .format("parquet")
  .queryName("coupon_sales_s")
  .trigger(Trigger.ProcessingTime("1 second"))
  .option("checkpointLocation", couponsCheckpointPath)
  .start(couponsOutputPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

untilStreamIsReady("coupon_sales")
assert(couponSalesQuery.isActive)
assert(dbutils.fs.ls(couponsOutputPath).size > 0)
assert(dbutils.fs.ls(couponsCheckpointPath).size > 0)
assert(couponSalesQuery.lastProgress.name.contains("coupon_sales"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Monitor streaming query
// MAGIC - Get ID of streaming query
// MAGIC - Get status of streaming query

// COMMAND ----------

// ANSWER
val queryID = couponSalesQuery.id

// COMMAND ----------

// ANSWER
val queryStatus = couponSalesQuery.status

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

import java.util.UUID
import org.apache.spark.sql.streaming.StreamingQueryStatus

assert(queryID.isInstanceOf[UUID])
assert(queryStatus.isInstanceOf[StreamingQueryStatus])

val statusStr = queryStatus.toString
assert(statusStr.contains("message") && statusStr.contains("isDataAvailable") && statusStr.contains("isTriggerActive"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Stop streaming query
// MAGIC - Stop the streaming query

// COMMAND ----------

// ANSWER
couponSalesQuery.stop()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(!couponSalesQuery.isActive)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6. Verify the records were written to a Parquet file

// COMMAND ----------

display(spark.read.parquet(couponsOutputPath))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Classroom Cleanup
// MAGIC Run the cell below to clean up resources.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
