// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reader & Writer
// MAGIC 1. Read from CSV files
// MAGIC 1. Read from JSON files
// MAGIC 1. Write DataFrame to files
// MAGIC 1. Write DataFrame to tables
// MAGIC 1. Write DataFrame to a Delta table
// MAGIC 
// MAGIC ##### Methods
// MAGIC - DataFrameReader (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">Python</a>/<a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html" target="_blank">Scala</a>): `csv`, `json`, `option`, `schema`
// MAGIC - DataFrameWriter (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">Python</a>/<a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameWriter.html" target="_blank">Scala</a>): `mode`, `option`, `parquet`, `format`, `saveAsTable`
// MAGIC - StructType (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType" target="_blank">Python</a>/<a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/types/StructType.html" target="_blank" target="_blank">Scala</a>): `toDDL`
// MAGIC 
// MAGIC ##### Spark Types
// MAGIC - Types (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#data-types" target="_blank">Python</a>/<a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/types/index.html" target="_blank">Scala</a>): `ArrayType`, `DoubleType`, `IntegerType`, `LongType`, `StringType`, `StructType`, `StructField`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read from CSV files
// MAGIC Read from CSV with DataFrameReader's `csv` method and the following options:
// MAGIC 
// MAGIC Tab separator, use first line as header, infer schema

// COMMAND ----------

val usersCsvPath = "/mnt/training/ecommerce/users/users-500k.csv"

val usersDF = spark.read
  .option("sep", "\t")
  .option("header", true)
  .option("inferSchema", true)
  .csv(usersCsvPath)

usersDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Manually define the schema by creating a `StructType` with column names and data types

// COMMAND ----------

import org.apache.spark.sql.types.{LongType, StringType, StructType, StructField}

val userDefinedSchema = StructType(Seq(
  StructField("user_id", StringType, true),
  StructField("user_first_touch_timestamp", LongType, true),
  StructField("email", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Read from CSV using this user-defined schema instead of inferring schema

// COMMAND ----------

val usersDF = spark.read
  .option("sep", "\t")
  .option("header", true)
  .schema(userDefinedSchema)
  .csv(usersCsvPath)

// COMMAND ----------

// MAGIC %md
// MAGIC Alternatively, define the schema using a DDL formatted string.

// COMMAND ----------

val DDLSchema = "user_id string, user_first_touch_timestamp long, email string"

val usersDF = spark.read
  .option("sep", "\t")
  .option("header", true)
  .schema(DDLSchema)
  .csv(usersCsvPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read from JSON files
// MAGIC 
// MAGIC Read from JSON with DataFrameReader's `json` method and the infer schema option

// COMMAND ----------

val eventsJsonPath = "/mnt/training/ecommerce/events/events-500k.json"

val eventsDF = spark.read
  .option("inferSchema", true)
  .json(eventsJsonPath)

eventsDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Read data faster by creating a `StructType` with the schema names and data types

// COMMAND ----------

import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField}

val userDefinedSchema = StructType(Seq(
  StructField("device", StringType, true),
  StructField("ecommerce", StructType(Seq(
    StructField("purchaseRevenue", DoubleType, true),
    StructField("total_item_quantity", LongType, true),
    StructField("unique_items", LongType, true)
  )), true),
  StructField("event_name", StringType, true),
  StructField("event_previous_timestamp", LongType, true),
  StructField("event_timestamp", LongType, true),
  StructField("geo", StructType(Seq(
    StructField("city", StringType, true),
    StructField("state", StringType, true)
  )), true),
  StructField("items", ArrayType(
    StructType(Seq(
      StructField("coupon", StringType, true),
      StructField("item_id", StringType, true),
      StructField("item_name", StringType, true),
      StructField("item_revenue_in_usd", DoubleType, true),
      StructField("price_in_usd", DoubleType, true),
      StructField("quantity", LongType, true)
    ))
  ), true),
  StructField("traffic_source", StringType, true),
  StructField("user_first_touch_timestamp", LongType, true),
  StructField("user_id", StringType, true)
))

val eventsDF = spark.read
  .schema(userDefinedSchema)
  .json(eventsJsonPath)

// COMMAND ----------

// MAGIC %md
// MAGIC You can use the `StructType` Scala method `toDDL` to have a DDL-formatted string created for you.
// MAGIC 
// MAGIC In Python, create a Scala cell to create the string to copy and paste.

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL

// COMMAND ----------

val DDLSchema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

val eventsDF = spark.read
  .schema(DDLSchema)
  .json(eventsJsonPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Write DataFrames to files
// MAGIC 
// MAGIC Write `usersDF` to parquet with DataFrameWriter's `parquet` method and the following configurations:
// MAGIC 
// MAGIC Snappy compression, overwrite mode

// COMMAND ----------

val usersOutputPath = workingDir + "/users.parquet"

usersDF.write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet(usersOutputPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Write DataFrames to tables
// MAGIC 
// MAGIC Write `eventsDF` to a table using the DataFrameWriter method `saveAsTable`
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This creates a global table, unlike the local view created by the DataFrame method `createOrReplaceTempView`

// COMMAND ----------

eventsDF.write.mode("overwrite").saveAsTable("events_s")

// COMMAND ----------

// MAGIC %md
// MAGIC This table was saved in the database created for you in classroom setup. See database name printed below.

// COMMAND ----------

println(databaseName)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Best Practice: Write Results to a Delta Table
// MAGIC 
// MAGIC In almost all cases, the best practice is to use <a href="https://delta.io/" target="_blank">Delta Lake</a>, especially whenever the data will be referenced from a Databricks Workspace. Data in Delta tables is stored in Parquet format.
// MAGIC 
// MAGIC Write `eventsDF` to Delta with DataFrameWriter's `save` method and the following configurations:
// MAGIC 
// MAGIC Delta format, overwrite mode

// COMMAND ----------

val eventsOutputPath = workingDir + "/delta/events"

eventsDF.write
  .format("delta")
  .mode("overwrite")
  .save(eventsOutputPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Ingesting Data Lab
// MAGIC 
// MAGIC Read in CSV files containing products data.
// MAGIC 1. Read with infer schema
// MAGIC 2. Read with user-defined schema
// MAGIC 3. Read with DDL formatted string
// MAGIC 4. Write to Delta

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Read with infer schema
// MAGIC - View the first CSV file using DBUtils method `fs.head` with the filepath provided in the variable `singleProductCsvFilePath`
// MAGIC - Create `productsDF` by reading from CSV files located in the filepath provided in the variable `productsCsvPath`
// MAGIC   - Configure options to use first line as header and infer schema

// COMMAND ----------

// ANSWER
val singleProductCsvFilePath = "/mnt/training/ecommerce/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
println(dbutils.fs.head(singleProductCsvFilePath))

val productsCsvPath = "/mnt/training/ecommerce/products/products.csv"

val productsDF = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv(productsCsvPath)

productsDF.printSchema()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(productsDF.count == 12)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Read with user-defined schema
// MAGIC Define schema by creating a `StructType` with column names and data types

// COMMAND ----------

// ANSWER
val userDefinedSchema = StructType(Seq(
  StructField("item_id", StringType, true),
  StructField("name", StringType, true),
  StructField("price", DoubleType, true)
))

val productsDF2 = spark.read
  .option("header", true)
  .schema(userDefinedSchema)
  .csv(productsCsvPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(userDefinedSchema.fieldNames.sameElements(Array("item_id", "name", "price")))

// COMMAND ----------

import org.apache.spark.sql.Row

val expected1 = Row("M_STAN_Q", "Standard Queen Mattress", 1045.0)
val result1 = productsDF2.first()

assert(expected1 == result1)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Read with DDL formatted string

// COMMAND ----------

// ANSWER
val DDLSchema = "`item_id` STRING,`name` STRING,`price` DOUBLE"

val productsDF3 = spark.read
  .option("header", true)
  .schema(DDLSchema)
  .csv(productsCsvPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(productsDF3.count == 12)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Write to Delta
// MAGIC Write `productsDF` to the filepath provided in the variable `productsOutputPath`

// COMMAND ----------

// ANSWER
val productsOutputPath = workingDir + "/delta/products"
productsDF.write
  .format("delta")
  .mode("overwrite")
  .save(productsOutputPath)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work

// COMMAND ----------

assert(dbutils.fs.ls(productsOutputPath).size == 5)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
