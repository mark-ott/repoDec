// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Aggregating Streams
// MAGIC 1. Add watermarking
// MAGIC 1. Aggregate with windows
// MAGIC 1. Display streaming query results
// MAGIC 1. Monitor streaming queries
// MAGIC 
// MAGIC ##### Classes
// MAGIC - DataStreamReader (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/DataStreamReader.html" target="_blank">Scala</a>)
// MAGIC - DataStreamWriter (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/DataStreamWriter.html" target="_blank">Scala</a>)
// MAGIC - StreamingQuery (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/StreamingQuery.html" target="_blank">Scala</a>)
// MAGIC - StreamingQueryManager (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQueryManager.html#pyspark.sql.streaming.StreamingQueryManager" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/StreamingQueryManager.html" target="_blank">Scala</a>)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Hourly Activity by Traffic Lab
// MAGIC Process streaming data to display the total active users by traffic source with a 1 hour window.
// MAGIC 1. Cast to timestamp and add watermark for 2 hours
// MAGIC 2. Aggregate active users by traffic source for 1 hour windows
// MAGIC 3. Execute query with display() and plot results
// MAGIC 5. Use query name to stop streaming query

// COMMAND ----------

// MAGIC %md
// MAGIC ### Setup
// MAGIC Run the cells below to generate hourly JSON files of event data for July 3, 2020.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

val schema = "device STRING, ecommerce STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT<city: STRING, state: STRING>, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING"

// hourly events logged from the BedBricks website on July 3, 2020
val hourlyEventsPath = "/mnt/training/ecommerce/events/events-2020-07-03.json"

val df = spark.readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .json(hourlyEventsPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Cast to timestamp and add watermark for 2 hours
// MAGIC - Add column **`createdAt`** by dividing **`event_timestamp`** by 1M and casting to timestamp
// MAGIC - Add watermark for 2 hours
// MAGIC 
// MAGIC Assign resulting DataFrame to **`eventsDF`**.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.col

val eventsDF = df.withColumn("createdAt", (col("event_timestamp") / 1e6).cast("timestamp"))
  .withWatermark("createdAt", "2 hours")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(eventsDF.schema.mkString("").contains("StructField(createdAt,TimestampType,true"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Aggregate active users by traffic source for 1 hour windows
// MAGIC - Set default shuffle partitions to number of cores on your cluster (not required, but runs faster)
// MAGIC - Group by **`traffic_source`** with a 1 hour window based on the **`createdAt`** column
// MAGIC - Aggregate the approximate count of distinct users and alias with "active_users"
// MAGIC - Select **`traffic_source`**, **`active_users`**, and the **`hour`** extracted from **`window.start`** with alias "hour"
// MAGIC - Sort by **`hour`**
// MAGIC 
// MAGIC Assign resulting DataFrame to **`trafficDF`**.

// COMMAND ----------

// ANSWER
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

import org.apache.spark.sql.functions.{approx_count_distinct, hour, window}

val trafficDF = eventsDF.groupBy(col("traffic_source"), window(col("createdAt"), "1 hour")).agg(
    approx_count_distinct("user_id").alias("active_users"))
  .select(col("traffic_source"), col("active_users"), hour(col("window.start")).alias("hour"))
  .sort("hour")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(trafficDF.schema.mkString("") == "StructField(traffic_source,StringType,true)StructField(active_users,LongType,false)StructField(hour,IntegerType,true)")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Execute query with display() and plot results
// MAGIC - Execute results for **`trafficDF`** using display()
// MAGIC   - Set the **`streamName`** parameter to set a name for the query
// MAGIC - Plot the streaming query results as a bar graph
// MAGIC - Configure the following plot options:
// MAGIC   - Keys: **`hour`**
// MAGIC   - Series groupings: **`traffic_source`**
// MAGIC   - Values: **`active_users`**

// COMMAND ----------

// ANSWER
display(trafficDF, streamName="hourly_traffic_s")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work
// MAGIC - The bar chart should plot `hour` on the x-axis and `active_users` on the y-axis
// MAGIC - Six bars should appear at every hour for all traffic sources
// MAGIC - The chart should stop at hour 23

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Manage streaming query
// MAGIC - Iterate over SparkSession's list of active streams to find one with name "hourly_traffic"
// MAGIC - Stop the streaming query

// COMMAND ----------

// ANSWER
untilStreamIsReady("hourly_traffic_s")

for (s <- spark.streams.active) {
  if (s.name == "hourly_traffic_s") {
    s.stop()
  }
}

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work
// MAGIC - Print all active streams to check "hourly_traffic" is no longer there

// COMMAND ----------

// MAGIC %md
// MAGIC ### Classroom Cleanup
// MAGIC Run the cell below to clean up resources.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
