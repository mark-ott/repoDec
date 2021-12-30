// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Aggregation
// MAGIC 
// MAGIC 1. Grouping data
// MAGIC 1. Grouped data methods
// MAGIC 1. Built-in aggregate functions
// MAGIC 
// MAGIC ##### Methods
// MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>): `groupBy`
// MAGIC - Grouped Data (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.html#pyspark.sql.GroupedData" target="_blank" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/RelationalGroupedDataset.html" target="_blank">Scala</a>): `agg`, `avg`, `count`, `max`, `sum`
// MAGIC - Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>): `approx_count_distinct`, `avg`, `sum`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use the BedBricks sales dataset.

// COMMAND ----------

val df = spark.read.parquet(eventsPath)
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Grouping data
// MAGIC Use the DataFrame `groupBy` method to create a grouped data object
// MAGIC 
// MAGIC This grouped data object is called `RelationalGroupedDataset` in Scala and `GroupedData` in Python

// COMMAND ----------

df.groupBy("event_name")

// COMMAND ----------

df.groupBy("geo.state", "geo.city")

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Grouped data methods
// MAGIC Various aggregate methods are available on the grouped data object

// COMMAND ----------

val eventCountsDF = df.groupBy("event_name").count()
display(eventCountsDF)

// COMMAND ----------

val avgStatePurchasesDF = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avgStatePurchasesDF)

// COMMAND ----------

val cityPurchaseQuantitiesDF = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity")
display(cityPurchaseQuantitiesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Built-in aggregate functions
// MAGIC Use the grouped data method `agg` to apply built-in aggregate functions
// MAGIC 
// MAGIC This allows you to apply other transformations on the resulting columns, such as `alias`

// COMMAND ----------

import org.apache.spark.sql.functions.sum

val statePurchasesDF = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(statePurchasesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Apply multiple aggregate functions on grouped data

// COMMAND ----------

import org.apache.spark.sql.functions.{avg, approx_count_distinct}

val stateAggregatesDF = df.groupBy("geo.state").agg(
  avg("ecommerce.total_item_quantity").alias("avg_quantity"),
  approx_count_distinct("user_id").alias("distinct_users"))

display(stateAggregatesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Revenue by Traffic Lab
// MAGIC Get the 3 traffic sources generating the highest total revenue.
// MAGIC 1. Aggregate revenue by traffic source
// MAGIC 2. Get top 3 traffic sources by total revenue
// MAGIC 3. Clean revenue columns to have two decimal places
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">DataFrame</a>: groupBy, sort, limit
// MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html" target="_blank">Column</a>: alias, desc, cast, operators
// MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Built-in Functions</a>: avg, sum

// COMMAND ----------

// MAGIC %md
// MAGIC ### Setup
// MAGIC Run the cell below to create the starting DataFrame **`df`**.

// COMMAND ----------

import org.apache.spark.sql.functions.col

// purchase events logged on the BedBricks website
val df = (spark.read.parquet(eventsPath)
  .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
  .filter(col("revenue").isNotNull)
  .drop("event_name"))

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Aggregate revenue by traffic source
// MAGIC - Group by **`traffic_source`**
// MAGIC - Get sum of **`revenue`** as **`total_rev`**
// MAGIC - Get average of **`revenue`** as **`avg_rev`**
// MAGIC 
// MAGIC Remember to import any necessary built-in functions.

// COMMAND ----------

// TODO

val trafficDF = df.FILL_IN

display(trafficDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

import org.apache.spark.sql.functions.round
val expected1 = Seq((12704560.0, 1083.175), (78800000.3, 983.2915), (24797837.0, 1076.6221), (47218429.0, 1086.8303), (16177893.0, 1083.4378), (8044326.0, 1087.218))
val result1 = {
  val testDF = trafficDF.sort("traffic_source").select(round($"total_rev", 4).alias("total_rev"), round($"avg_rev", 4).alias("avg_rev"))
  for (row <- testDF.collect()) yield (row.getDouble(0), row.getDouble(1))}

assert(expected1.sameElements(result1))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Get top three traffic sources by total revenue
// MAGIC - Sort by **`total_rev`** in descending order
// MAGIC - Limit to first three rows

// COMMAND ----------

// TODO
val topTrafficDF = trafficDF.FILL_IN

display(topTrafficDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

import org.apache.spark.sql.functions.round
val expected2 = Seq((78800000.3, 983.2915), (47218429.0, 1086.8303), (24797837.0, 1076.6221))
val result2 = {
  val testDF = topTrafficDF.select(round($"total_rev", 4).alias("total_rev"), round($"avg_rev", 4).alias("avg_rev"))
  for (row <- testDF.collect()) yield (row.getDouble(0), row.getDouble(1))}

assert(expected2.sameElements(result2))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Limit revenue columns to two decimal places
// MAGIC - Modify columns **`avg_rev`** and **`total_rev`** to contain numbers with two decimal places
// MAGIC   - Use **`withColumn()`** with the same names to replace these columns
// MAGIC   - To limit to two decimal places, multiply each column by 100, cast to long, and then divide by 100

// COMMAND ----------

// TODO
val finalDF = topTrafficDF.FILL_IN

display(finalDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

val expected3 = Seq((78800000.29, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62))
val result3 = for (row <- finalDF.collect()) yield (row.getDouble(1), row.getDouble(2))

assert(expected3.sameElements(result3))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Bonus: Rewrite using a built-in math function
// MAGIC Find a built-in math function that rounds to a specified number of decimal places

// COMMAND ----------

// TODO
val bonusDF = topTrafficDF.FILL_IN

display(bonusDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

val expected4 = Seq((78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62))
val result4 = for (row <- bonusDF.collect()) yield (row.getDouble(1), row.getDouble(2))

assert(expected4.sameElements(result4))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Chain all the steps above

// COMMAND ----------

// TODO
val chainDF = df.FILL_IN

display(chainDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

val expected5 = Seq((78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62))
val result5 = for (row <- chainDF.collect()) yield (row.getDouble(1), row.getDouble(2))

assert(expected5.sameElements(result5))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
