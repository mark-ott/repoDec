# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Abandoned Carts Lab
# MAGIC Get abandoned cart items for email without purchases.
# MAGIC 1. Get emails of converted users from transactions
# MAGIC 2. Join emails with user IDs
# MAGIC 3. Get cart item history for each user
# MAGIC 4. Join cart item history with emails
# MAGIC 5. Filter for emails with abandoned cart items
# MAGIC 
# MAGIC ##### Methods
# MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>): `join`
# MAGIC - Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>): `lit`
# MAGIC - DataFrameNaFunctions (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameNaFunctions.html" target="_blank">Scala</a>): `fill`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cells below to create DataFrames **`salesDF`**, **`usersDF`**, and **`eventsDF`**.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# sale transactions at BedBricks
salesDF = spark.read.parquet(salesPath)
display(salesDF)

# COMMAND ----------

# user IDs and emails at BedBricks
usersDF = spark.read.parquet(usersPath)
display(usersDF)

# COMMAND ----------

# events logged on the BedBricks website
eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Get emails of converted users from transactions
# MAGIC - Select **`email`** column in **`salesDF`** and remove duplicates
# MAGIC - Add new column **`converted`** with value **`True`** for all rows
# MAGIC 
# MAGIC Save result as **`convertedUsersDF`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import *

convertedUsersDF = (salesDF.select("email")
  .distinct()
  .withColumn("converted", lit(True))
)
display(convertedUsersDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Join emails with user IDs
# MAGIC - Perform an outer join on **`convertedUsersDF`** and **`usersDF`** with the **`email`** field
# MAGIC - Filter for users where **`email`** is not null
# MAGIC - Fill null values in **`converted`** as **`False`**
# MAGIC 
# MAGIC Save result as **`conversionsDF`**.

# COMMAND ----------

# ANSWER
conversionsDF = (usersDF.join(convertedUsersDF, "email", "outer")
  .filter(col("email").isNotNull())
  .na.fill(False)
)
display(conversionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Get cart item history for each user
# MAGIC - Explode **`items`** field in **`eventsDF`**
# MAGIC - Group by **`user_id`**
# MAGIC   - Collect set of all **`items.item_id`** objects for each user and alias with "cart"
# MAGIC 
# MAGIC Save result as **`cartsDF`**.

# COMMAND ----------

# ANSWER
cartsDF = (eventsDF.withColumn("items", explode("items"))
  .groupBy("user_id").agg(collect_set("items.item_id").alias("cart"))
)
display(cartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Join cart item history with emails
# MAGIC - Perform a left join on **`conversionsDF`** and **`cartsDF`** on the **`user_id`** field
# MAGIC 
# MAGIC Save result as **`emailCartsDF`**.

# COMMAND ----------

# ANSWER
emailCartsDF = conversionsDF.join(cartsDF, "user_id", "left")
display(emailCartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Filter for emails with abandoned cart items
# MAGIC - Filter **`emailCartsDF`** for users where **`converted`** is False
# MAGIC - Filter for users with non-null carts
# MAGIC 
# MAGIC Save result as **`abandonedItemsDF`**.

# COMMAND ----------

# ANSWER
abandonedCartsDF = (emailCartsDF.filter(col("converted") == False)
  .filter(col("cart").isNotNull())
)
display(abandonedCartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus: Plot number of abandoned cart items by product

# COMMAND ----------

# ANSWER
abandonedItemsDF = (abandonedCartsDF.withColumn("items", explode("cart"))
  .groupBy("items").count()
  .sort("items")
)
display(abandonedItemsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup
