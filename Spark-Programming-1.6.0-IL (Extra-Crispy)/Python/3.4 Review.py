# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # DataFrames and Transformations Review
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) De-Duping Data Lab
# MAGIC 
# MAGIC In this exercise, we're doing ETL on a file we've received from a customer. That file contains data about people, including:
# MAGIC 
# MAGIC * first, middle and last names
# MAGIC * gender
# MAGIC * birth date
# MAGIC * Social Security number
# MAGIC * salary
# MAGIC 
# MAGIC But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:
# MAGIC 
# MAGIC * In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL").
# MAGIC * The Social Security numbers aren't consistent either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").
# MAGIC 
# MAGIC The name fields are guaranteed to match, if you disregard character case, and the birth dates will also match. The salaries will match as well,
# MAGIC and the Social Security Numbers *would* match if they were somehow put in the same format.
# MAGIC 
# MAGIC Your job is to remove the duplicate records. The specific requirements of your job are:
# MAGIC 
# MAGIC * Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
# MAGIC * Preserve the data format of the columns. For example, if you write the first name column in all lowercase, you haven't met this requirement.
# MAGIC 
# MAGIC Next, you will write the results using two methods:
# MAGIC 
# MAGIC * First, write the result as a Parquet file, as designated by *destFile*. It must contain 8 part files (8 files ending in ".parquet").
# MAGIC * Next, as is the best practice, write the result to Delta as designated by the filepath *deltaDestFile*.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** The initial dataset contains 103,000 records.<br/>
# MAGIC The de-duplicated result has 100,000 records.
# MAGIC 
# MAGIC ##### Methods
# MAGIC - DataFrameReader (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html" target="_blank">Scala</a>)
# MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>)
# MAGIC - Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>)
# MAGIC - DataFrameWriter (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">Python</a>/<a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameWriter.html" target="_blank">Scala</a>)

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC It's helpful to look at the file first, so you can check the format. `dbutils.fs.head()` (or just `%fs head`) is a big help here.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/dataframes/people-with-dups.txt

# COMMAND ----------

# TODO

sourceFile = "dbfs:/mnt/training/dataframes/people-with-dups.txt"
destFile = workingDir + "/people.parquet"

# In case it already exists
dbutils.fs.rm(destFile, True)

# Complete your work here...


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work
# MAGIC 
# MAGIC Verify that you wrote the parquet file out to **destFile** and that you have the right number of records.

# COMMAND ----------

partFiles = len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls(destFile))))

finalDF = spark.read.parquet(destFile)
finalCount = finalDF.count()

assert partFiles == 8, "expected 8 parquet files located in destFile"
assert finalCount == 100000, "expected 100000 records in finalDF"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up classroom
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"
