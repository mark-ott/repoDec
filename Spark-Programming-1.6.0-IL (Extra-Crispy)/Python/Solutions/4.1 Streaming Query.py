# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Query
# MAGIC 1. Build streaming DataFrames
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results
# MAGIC 1. Monitor streaming query
# MAGIC 
# MAGIC ##### Classes
# MAGIC - DataStreamReader (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/DataStreamReader.html" target="_blank">Scala</a>)
# MAGIC - DataStreamWriter (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/DataStreamWriter.html" target="_blank">Scala</a>)
# MAGIC - StreamingQuery (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/StreamingQuery.html" target="_blank">Scala</a>)

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

schema = "device STRING, ecommerce STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT<city: STRING, state: STRING>, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING"

df = (spark.readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet(eventsPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Build streaming DataFrames

# COMMAND ----------

df.isStreaming

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count

emailTrafficDF = (df.filter(col("traffic_source") == "email")
  .withColumn("mobile", col("device").isin(["iOS", "Android"]))
  .select("user_id", "event_timestamp", "mobile")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Write streaming query results

# COMMAND ----------

checkpointPath = userhome + "/email_traffic/checkpoint"
outputPath = userhome + "/email_traffic/output"

devicesQuery = (emailTrafficDF.writeStream
  .outputMode("append")
  .format("parquet")
  .queryName("email_traffic_p")
  .trigger(processingTime="1 second")
  .option("checkpointLocation", checkpointPath)
  .start(outputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Monitor streaming query

# COMMAND ----------

devicesQuery.id

# COMMAND ----------

devicesQuery.status

# COMMAND ----------

devicesQuery.awaitTermination(5)

# COMMAND ----------

devicesQuery.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Coupon Sales Lab
# MAGIC Process and append streaming data on transactions using coupons.
# MAGIC 1. Read data stream
# MAGIC 2. Filter for transactions with coupons codes
# MAGIC 3. Write streaming query results to parquet
# MAGIC 4. Monitor streaming query
# MAGIC 5. Stop streaming query
# MAGIC 
# MAGIC ##### Classes
# MAGIC - [DataStreamReader](http://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/streaming/DataStreamReader.html)
# MAGIC - [DataStreamWriter](http://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/streaming/DataStreamWriter.html)
# MAGIC - [StreamingQuery](http://spark.apache.org/docs/3.0.0/api/scala/org/apache/spark/sql/streaming/StreamingQuery.html)

# COMMAND ----------

schema = "order_id BIGINT, email STRING, transaction_timestamp BIGINT, total_item_quantity BIGINT, purchase_revenue_in_usd DOUBLE, unique_items BIGINT, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read data stream
# MAGIC - Use schema stored in **`schema`**
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from parquet with filepath stored in **`salesPath`**
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`df`**

# COMMAND ----------

# ANSWER
df = (spark.readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet(salesPath)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

# COMMAND ----------

assert df.isStreaming
assert df.columns == ['order_id', 'email', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'items']

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Filter for transactions with coupon codes
# MAGIC - Explode **`items`** field in **`df`**
# MAGIC - Filter for records where **`items.coupon`** is not null
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`couponSalesDF`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, explode

couponSalesDF = (df.withColumn("items", explode(col("items")))
  .filter(col("items.coupon").isNotNull())
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

# COMMAND ----------

schemaStr = str(couponSalesDF.schema)
assert "StructField(items,StructType(List(StructField(coupon" in schemaStr, "items column was not exploded"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write streaming query results to parquet
# MAGIC - Configure streaming query to write out to parquet in "append" mode
# MAGIC - Set query name to "coupon_sales"
# MAGIC - Set a trigger interval of 1 second
# MAGIC - Set checkpoint location to **`couponsCheckpointPath`**
# MAGIC - Set output filepath to **`couponsOutputPath`**
# MAGIC 
# MAGIC Assign the resulting streaming query to **`couponSalesQuery`**.

# COMMAND ----------

# ANSWER

couponsCheckpointPath = workingDir + "/coupon-sales/checkpoint"
couponsOutputPath = workingDir + "/coupon-sales/output"

couponSalesQuery = (couponSalesDF.writeStream
  .outputMode("append")
  .format("parquet")
  .queryName("coupon_sales_p")
  .trigger(processingTime="1 second")
  .option("checkpointLocation", couponsCheckpointPath)
  .start(couponsOutputPath)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

# COMMAND ----------

untilStreamIsReady("coupon_sales")
assert couponSalesQuery.isActive
assert len(dbutils.fs.ls(couponsOutputPath)) > 0
assert len(dbutils.fs.ls(couponsCheckpointPath)) > 0
assert "coupon_sales" in couponSalesQuery.lastProgress["name"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Monitor streaming query
# MAGIC - Get ID of streaming query
# MAGIC - Get status of streaming query

# COMMAND ----------

# ANSWER
queryID = couponSalesQuery.id
print(queryID)

# COMMAND ----------

# ANSWER
queryStatus = couponSalesQuery.status
print(queryStatus)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

# COMMAND ----------

assert type(queryID) == str
assert list(queryStatus.keys()) == ['message', 'isDataAvailable', 'isTriggerActive']

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Stop streaming query
# MAGIC - Stop the streaming query

# COMMAND ----------

# ANSWER
couponSalesQuery.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

# COMMAND ----------

assert not couponSalesQuery.isActive

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Verify the records were written to a Parquet file

# COMMAND ----------

display(spark.read.parquet(couponsOutputPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup
