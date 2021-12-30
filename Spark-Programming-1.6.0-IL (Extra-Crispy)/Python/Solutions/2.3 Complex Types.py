# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Complex Types
# MAGIC 
# MAGIC ##### Methods
# MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>): `union`
# MAGIC - Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>):
# MAGIC   - Collection: `explode`, `array_contains`, `element_at`, `collect_set`
# MAGIC   - String: `split`

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) User Purchases
# MAGIC List all size and quality options purchased by each buyer.
# MAGIC 1. Extract item details from purchases
# MAGIC 2. Extract size and quality options from mattress purchases
# MAGIC 3. Extract size and quality options from pillow purchases
# MAGIC 4. Combine data for mattress and pillows
# MAGIC 5. List all size and quality options bought by each user

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

df = spark.read.parquet(salesPath)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Extract item details from purchases
# MAGIC - Explode **`items`** field in **`df`**
# MAGIC - Select **`email`** and **`item.item_name`** fields
# MAGIC - Split words in **`item_name`** into an array and alias with "details"
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`detailsDF`**.

# COMMAND ----------

from pyspark.sql.functions import *

detailsDF = (df.withColumn("items", explode("items"))
  .select("email", "items.item_name")
  .withColumn("details", split(col("item_name"), " "))
)
display(detailsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Extract size and quality options from mattress purchases
# MAGIC - Filter **`detailsDF`** for records where **`details`** contains "Mattress"
# MAGIC - Add **`size`** column from extracting element at position 2
# MAGIC - Add **`quality`** column from extracting element at position 1
# MAGIC 
# MAGIC Save result as **`mattressDF`**.

# COMMAND ----------

mattressDF = (detailsDF.filter(array_contains(col("details"), "Mattress"))
  .withColumn("size", element_at(col("details"), 2))
  .withColumn("quality", element_at(col("details"), 1))
)
display(mattressDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Extract size and quality options from pillow purchases
# MAGIC - Filter **`detailsDF`** for records where **`details`** contains "Pillow"
# MAGIC - Add **`size`** column from extracting element at position 1
# MAGIC - Add **`quality`** column from extracting element at position 2
# MAGIC 
# MAGIC Note the positions of **`size`** and **`quality`** are switched for mattresses and pillows.
# MAGIC 
# MAGIC Save result as **`pillowDF`**.

# COMMAND ----------

pillowDF = (detailsDF.filter(array_contains(col("details"), "Pillow"))
  .withColumn("size", element_at(col("details"), 1))
  .withColumn("quality", element_at(col("details"), 2))
)
display(pillowDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Combine data for mattress and pillows
# MAGIC - Perform a union on **`mattressDF`** and **`pillowDF`** by column names
# MAGIC - Drop **`details`** column
# MAGIC 
# MAGIC Save result as **`unionDF`**.

# COMMAND ----------

unionDF = (mattressDF.unionByName(pillowDF)
  .drop("details"))
display(unionDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. List all size and quality options bought by each user
# MAGIC - Group rows in **`unionDF`** by **`email`**
# MAGIC   - Collect set of all items in **`size`** for each user with alias "size options"
# MAGIC   - Collect set of all items in **`quality`** for each user with alias "quality options"
# MAGIC 
# MAGIC Save result as **`optionsDF`**.

# COMMAND ----------

optionsDF = (unionDF.groupBy("email")
  .agg(collect_set("size").alias("size options"),
       collect_set("quality").alias("quality options"))
)
display(optionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup
