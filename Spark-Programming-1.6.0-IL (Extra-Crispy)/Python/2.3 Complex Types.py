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

# detailsDF.orderBy(col("email")).show(1000, truncate = False)

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

# Since Schema matched exactly between 'Mattress' and 'Pillow', could have just used 'union'
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

# COMMAND ----------

########################
# Bonus Lab: 'explode' #
########################

# 'explode': Returns a new row for each element in the given Array
#            Returns a new column for each element in the given Map

from pyspark.sql import Row
eDF = spark.createDataFrame([Row(a=1, arraylist=[1,2,3], mapfield={"a": "b"})])
display(eDF)

# 'explode' for an Array (1 -> Many rows for Array) 
# Returns a new row for each element in the given Array
# Uses the default column name col for elements in the Array and key and value for elements in the Map unless specified otherwise
# 'explode' for an Array data type
display(eDF.select(eDF.arraylist, explode(eDF.arraylist).alias("explode_array")))
               
# 'explode' for a Map data type (1 -> Many columns for Map)
display(eDF.select(eDF.mapfield, explode(eDF.mapfield))) 


# COMMAND ----------

###############################
# Bonus Lab: 'array_contains' #
###############################

# 'array_contains': Returns Null if the array is Null, 'true' if the array contains the given value, and False otherwise

#  Here's the data we'll be using for 'array_contains'
df = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])
display(df)           

# Return 'true' or 'false' for thaat row
display(df.select(df.data, array_contains(df.data, "a").alias("my_array")))

# Returns row(s) that are only 'true' (via 'filter')
display(df.filter(array_contains(df.data, "b")))

# Returns No Results since no rows are 'true'
display(df.filter(array_contains(df.data, "d")))

# COMMAND ----------

###########################
# Bonus Lab: 'element_at' #
###########################

# 'element_at'. Returns element of Array at given index if col is Array
#               Returns value for the given key if col is Map 
#               Based on Index 1

# Build Dataset
df = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])
display(df)

# Array example: Returns element at given index (Index 1) for Column
display(df.select(df.data, element_at(df.data, 1).alias("IndexValueForPosition1")))
#        

# Build Dataset
from pyspark.sql.functions import *
#df1 = spark.createDataFrame([({"a": 1, "b": 2},), ({},)], ['data'])
display(df1)

# Map example: Find 'value' for given Index Key = 'a'
display(df1.select(element_at(df1.data, lit("a")).alias("valueOfGivenKey"))) 

# COMMAND ----------

############################
# Bonus Lab: 'collect_set' #
############################

# 'collect_set': Aggregate function: Returns a set of objects with duplicate elements eliminated

# Build Dataset
df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
display(df2)  

df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
display(df2.agg(collect_set('age'))) 

# COMMAND ----------

######################
# Bonus Lab: 'slice' #
######################

# 'slice': Returns an Array containing all the elements in x from Index start 
# (or starting from the end if start is negative) with the specified length   
# (Based on Index 1)

# Here's the Dataset
from pyspark.sql.functions import *
df = spark.createDataFrame([([1, 2, 3, 4],), ([5, 6],)], ['col1'])
display(df)

# # In our example, 'slice' starting at Index 2, return up to length of 3 elements for 'col1'
display(df.select(slice(df.col1, 2, 3).alias("sliced")))  

# COMMAND ----------

############################
# Bonus Lab: 'unionByName' #
############################

# Here's the dta we'll be using for 'unionByName'
# Difference between this function and union() is that this function resolves columns by name (not by position):

# Build Datasets
df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col0"])

# Union by Name
df1.unionByName(df2).show()
