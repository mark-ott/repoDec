// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Query Optimization
// MAGIC 1. Logical optimizations
// MAGIC 1. Predicate pushdown
// MAGIC 1. No predicate pushdown
// MAGIC 
// MAGIC ##### Methods
// MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>): 'explain'

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

val df = spark.read.parquet(eventsPath)
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Logical Optimization

// COMMAND ----------

import org.apache.spark.sql.functions.col

val limitEventsDF = df
  .filter(col("event_name") =!= "reviews")
  .filter(col("event_name") =!= "checkout")
  .filter(col("event_name") =!= "register")
  .filter(col("event_name") =!= "email_coupon")
  .filter(col("event_name") =!= "cc_info")
  .filter(col("event_name") =!= "delivery")
  .filter(col("event_name") =!= "shipping_info")
  .filter(col("event_name") =!= "press")


limitEventsDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### `explain(..)`
// MAGIC 
// MAGIC Prints the plans (logical and physical), optionally formatted by a given explain mode

// COMMAND ----------

limitEventsDF.explain(true)

// COMMAND ----------

val betterDF = df.filter(
  (col("event_name").isNotNull) &&
  (col("event_name") =!= "reviews") &&
  (col("event_name") =!= "checkout") &&
  (col("event_name") =!= "register") &&
  (col("event_name") =!= "email_coupon") &&
  (col("event_name") =!= "cc_info") &&
  (col("event_name") =!= "delivery") &&
  (col("event_name") =!= "shipping_info") &&
  (col("event_name") =!= "press")
)

betterDF.count()

betterDF.explain(true)

// COMMAND ----------

val stupidDF = df
  .filter(col("event_name") =!= "finalize")
  .filter(col("event_name") =!= "finalize")
  .filter(col("event_name") =!= "finalize")
  .filter(col("event_name") =!= "finalize")
  .filter(col("event_name") =!= "finalize")

stupidDF.explain(true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Predicate Pushdown
// MAGIC 
// MAGIC Here is example with JDBC where predicate pushdown takes place

// COMMAND ----------

// MAGIC %scala
// MAGIC // Ensure that the driver class is loaded
// MAGIC Class.forName("org.postgresql.Driver")

// COMMAND ----------

val jdbcURL = "jdbc:postgresql://54.213.33.240/training"

// Username and Password w/read-only rights
val connProperties = new java.util.Properties()
connProperties.put("user", "training")
connProperties.put("password", "training")

val ppDF = spark.read.jdbc(
    jdbcURL,                      // the JDBC URL
    "training.people_1m",         // the name of the table
    "id",                         // the name of a column of an integral type that will be used for partitioning
    1,                            // the minimum value of columnName used to decide partition stride
    1000000,                      // the maximum value of columnName used to decide partition stride
    8,                            // the number of partitions/connections
    connProperties                // the connection properties
  )
  .filter($"gender" === "M")      // Filter the data by gender

// COMMAND ----------

ppDF.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Note the lack of a **Filter** and the presence of a **PushedFilters** in the **Scan**

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) No Predicate Pushdown
// MAGIC 
// MAGIC This will make a little more sense if we **compare it to examples** that don't push down the filter.

// COMMAND ----------

// MAGIC %md
// MAGIC Caching the data before filtering eliminates the possibility for the predicate push down

// COMMAND ----------

val cachedDF = spark.read
  .jdbc(
    jdbcURL,                      // the JDBC URL
    "training.people_1m",         // the name of the table
    "id",                         // the name of a column of an integral type that will be used for partitioning
    1,                            // the minimum value of columnName used to decide partition stride
    1000000,                      // the maximum value of columnName used to decide partition stride
    8,                            // the number of partitions/connections
    connProperties                // the connection properties
  )

cachedDF.cache().count()     // materialize the cache

val filteredDF = cachedDF.filter($"gender" === "M")      // Filter the data by gender

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the **Scan** (the JDBC read) we saw in the previous example, here we also see the **InMemoryTableScan** followed by a **Filter** in the explain plan.
// MAGIC 
// MAGIC This means Spark had to filter ALL the data from RAM instead of in the Database.

// COMMAND ----------

filteredDF.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Here is another example using CSV where predicate pushdown does **not** place

// COMMAND ----------

val csvDF = (spark.read
  .option("header", "true")
  .option("sep", "\t")
  .option("inferSchema", "true")
  .csv("/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv")
  .filter(col("site") === "desktop"))

// COMMAND ----------

// MAGIC %md
// MAGIC Note the presence of a **Filter** and **PushedFilters** in the **FileScan csv**
// MAGIC 
// MAGIC Again, we see **PushedFilters** because Spark is *trying* to push down to the CSV file.
// MAGIC 
// MAGIC However, this does not work here, and thus we see, like in the last example, we have a **Filter** after the **FileScan**, actually an **InMemoryFileIndex**.

// COMMAND ----------

csvDF.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
