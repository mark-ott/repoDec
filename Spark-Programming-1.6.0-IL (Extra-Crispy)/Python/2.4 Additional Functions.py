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

# Sale transactions at BedBricks (Customers who had Cart Purchase)
salesDF = spark.read.parquet(salesPath)
display(salesDF)

# COMMAND ----------

# user IDs and emails at BedBricks
usersDF = spark.read.parquet(usersPath)
display(usersDF)

# COMMAND ----------

# events logged on the BedBricks website ('items' Column = Products put/did not put in Cart)
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

# TODO
# Here are emails that had a Purchase
convertedUsersDF = (salesDF.FILL_IN
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

# TODO
# Now we have users who have/have not be 'converted'
conversionsDF = (usersDF.FILL_IN
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

# TODO
# Users who had populated 'Cart'
cartsDF = (eventsDF.FILL_IN
)
display(cartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Join cart item history with emails
# MAGIC - Perform a left join on **`conversionsDF`** and **`cartsDF`** on the **`user_id`** field
# MAGIC 
# MAGIC Save result as **`emailCartsDF`**.

# COMMAND ----------

# TODO
# Now we know Users, what was in their 'cart', and if they 'converted' or not
# Note it's possible for user to have 'converted' = true and cart = NULL since we dropped Duplicate user_id
emailCartsDF = conversionsDF.FILL_IN
display(emailCartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Filter for emails with abandoned cart items
# MAGIC - Filter **`emailCartsDF`** for users where **`converted`** is False
# MAGIC - Filter for users with non-null carts
# MAGIC 
# MAGIC Save result as **`abandonedItemsDF`**.

# COMMAND ----------

# TODO
# Find which users had 'converted' = False and non-Null 'cart'. These are abandoned Carts
abandonedItemsDF = (abandonedCartsDF.FILL_IN
)
display(abandonedItemsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus: Plot number of abandoned cart items by product

# COMMAND ----------

abandonedItemsDF = (abandonedCartsDF.withColumn("items", explode("cart"))
                                    .groupBy("items").count()
                                    .sort("items")
)
                    
display(abandonedItemsDF)

# Bonus: Sort by 'count' instead to get Better chart
#abandonedItemsDF = (abandonedCartsDF.withColumn("items", explode("cart"))
#                                    .groupBy("items").count()
#                                    .sort("count")
#)
                    
#display(abandonedItemsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# Bonus: JOIN syntax
emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","mgr","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

display(empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id, "inner"))

# COMMAND ----------

# 1. Bonus: Get emails of converted users from transactions
from pyspark.sql.functions import *

convertedUsersDF = (salesDF.select("email")
           .dropDuplicates()
           .withColumn("converted", lit("true"))
)
display(convertedUsersDF)

# COMMAND ----------

# 2. Join emails with user IDs
# Bonus: All Users with emails who have either Purchased ('converted' = true) or Not ('converted' = false)
conversionsDF = (convertedUsersDF.join(usersDF, "email", "outer")
   .filter(col("email").isNotNull())
   .na.fill("false")
                )

display(conversionsDF)

# COMMAND ----------

# 3. Bonus: Get cart item history for each user
cartsDF = (eventsDF.withColumn("items", explode("items"))
                   .groupBy("user_id")
                   .agg(collect_set(col("items.item_id")).alias("cart"))
          )

display(cartsDF)

# COMMAND ----------

# Step 4. Join cart item history with emails
# Bonus: Do Left Join to get Customers who did/did not Purchase
emailCartsDF = conversionsDF.join(cartsDF, "user_id", "left")
display(emailCartsDF)

# COMMAND ----------

# Step 5: 
# Bonus: Filter for emails with abandoned cart items
abandonedCartsDF = (emailCartsDF.filter((col("converted") == "false") & (col("cart").isNotNull())))                  
display(abandonedCartsDF)
