# Databricks notebook source
# MAGIC 
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbtest import *
from dbacademy.dbrest import DBAcademyRestClient

# Multiple references throug test code
name = "Spark Programming"

try:
  spark_versions_param = dbutils.widgets.get("spark_versions")
except:
  # Default to the current LTS
  spark_versions_param = "7.3.x-scala2.12"
  
spark_versions = spark_versions_param.split(",")  
spark_versions = list(map(lambda s: s.strip(), spark_versions))
  
i3_xlarge_pool = "1117-113806-juice922-pool-JxljQpdx"
i3_2xlarge_pool = "0428-041417-atop1-pool-g1Bkn407"
d3_v2_pool = "1117-121723-show503-pool-ubXZ7gOS"

test_config = TestConfig(
  name,                                                                            # The name of the course
  spark_version = spark_versions[0],                                               # Current version
  workers = 0,                                                                     # Test in local mode
  instance_pool = i3_2xlarge_pool if dbgems.get_cloud() == "AWS" else d3_v2_pool,  # AWS or MSA instance pool
  libraries = [ ],                                                                 # No required libraries
  results_table = name.lower().replace(" ", "_"),                                  # The name of the table results will be logged to
)

print("Test Configuration")
test_config.print()

print("\nSpark Versions:")
for version in spark_versions:
  print(" ", version)
  

# COMMAND ----------

# REST client used for varios operations
client = DBAcademyRestClient()

def test_with_version(spark_version, language):  
  test_config.spark_version = spark_version
  test_version = spark_version.split("-")[0]

  # The only notebooks we can test effectively come from the Solutions
  # folder, all other notebooks are expeted to have some type of failure
  # due to the fact that labs must first be compelted by students
  # See /Build/Test-All-Published
  home = f"{dbgems.get_notebook_dir(-3)}/{language}/Solutions"

  jobs = {
    f"[TEST] {name} #1.2 {language} {test_version}": (f"{home}/1.2 Databricks Platform", 0, 0),
    f"[TEST] {name} #1.3 {language} {test_version}": (f"{home}/1.3 Spark SQL", 0, 0),
    f"[TEST] {name} #1.4 {language} {test_version}": (f"{home}/1.4 Reader & Writer", 0, 0),
    f"[TEST] {name} #1.5 {language} {test_version}": (f"{home}/1.5 DataFrame & Column", 0, 0),

    f"[TEST] {name} #2.1 {language} {test_version}": (f"{home}/2.1 Aggregation", 0, 0),
    f"[TEST] {name} #2.2 {language} {test_version}": (f"{home}/2.2 Datetimes", 0, 0),
    f"[TEST] {name} #2.3 {language} {test_version}": (f"{home}/2.3 Complex Types", 0, 0),
    f"[TEST] {name} #2.4 {language} {test_version}": (f"{home}/2.4 Additional Functions", 0, 0),
    f"[TEST] {name} #2.5 {language} {test_version}": (f"{home}/2.5 UDFs", 0, 0),

    f"[TEST] {name} #3.2 {language} {test_version}": (f"{home}/3.2 Query Optimization", 0, 0),
    f"[TEST] {name} #3.3 {language} {test_version}": (f"{home}/3.3 Partitioning", 0, 0),
    f"[TEST] {name} #3.4 {language} {test_version}": (f"{home}/3.4 Review", 0, 0),

    f"[TEST] {name} #4.1 {language} {test_version}": (f"{home}/4.1 Streaming Query", 0, 0),
    f"[TEST] {name} #4.2 {language} {test_version}": (f"{home}/4.2 Aggregating Streams", 0, 0),
    f"[TEST] {name} #4.3 {language} {test_version}": (f"{home}/4.3 Delta Lake", 0, 0),
    f"[TEST] {name} #4.4 {language} {test_version}": (f"{home}/OPTIONAL Processing Streams", 0, 0),
  }
  
  client.jobs().delete_by_name(jobs, success_only=False)

  test_all_notebooks(jobs, test_config)
  wait_for_notebooks(test_config, jobs, fail_fast=False)

  client.jobs().delete_by_name(jobs, success_only=True)  

# COMMAND ----------

for version in spark_versions:
  # Test all of python in parallel
  test_with_version(version, "Python")
  
  # But wait until python is done before running Scala in parallel
  test_with_version(version, "Scala")

# COMMAND ----------

display(spark.sql(f"select * from {test_config.results_table} where suite_id = '{test_config.suite_id}' order by job_id, run_id"))

