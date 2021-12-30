// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Databricks Platform
// MAGIC 1. Execute code in multiple languages
// MAGIC 1. Create documentation cells
// MAGIC 1. Access DBFS (Databricks File System)
// MAGIC 1. Create database and table
// MAGIC 1. Query table and plot results
// MAGIC 1. Add notebook parameters with widgets
// MAGIC 
// MAGIC ##### Databricks Notebook Utilities
// MAGIC - <a href="https://docs.databricks.com/notebooks/notebooks-use.html#language-magic" target="_blank">Magic commands</a>: `%python`, `%scala`, `%sql`, `%r`, `%sh`, `%md`
// MAGIC - <a href="https://docs.databricks.com/dev-tools/databricks-utils.html" target="_blank">DBUtils</a>: `dbutils.fs` (`%fs`), `dbutils.notebooks` (`%run`), `dbutils.widgets`
// MAGIC - <a href="https://docs.databricks.com/notebooks/visualizations/index.html" target="_blank">Visualization</a>: `display`, `displayHTML`

// COMMAND ----------

// MAGIC %md
// MAGIC ### Setup
// MAGIC Run classroom setup to mount Databricks training datasets and create your own database for BedBricks.
// MAGIC 
// MAGIC Use the `%run` magic command to run another notebook within a notebook

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Execute code in multiple languages
// MAGIC Run default language of notebook

// COMMAND ----------

println("Run default language")

// COMMAND ----------

// MAGIC %md
// MAGIC Run language specified by language magic command: `%python`, `%scala`, `%sql`, `%r`

// COMMAND ----------

// MAGIC %python
// MAGIC print("Run python")

// COMMAND ----------

// MAGIC %scala
// MAGIC println("Run scala")

// COMMAND ----------

// MAGIC %sql
// MAGIC select "Run SQL"

// COMMAND ----------

// MAGIC %r
// MAGIC print("Run R", quote=FALSE)

// COMMAND ----------

// MAGIC %md
// MAGIC Run shell commands using magic command: `%sh`

// COMMAND ----------

// MAGIC %sh ps | grep 'java'

// COMMAND ----------

// MAGIC %md
// MAGIC Render HTML using the function: `displayHTML` (available in Python, Scala, and R)

// COMMAND ----------

val html = """<h1 style="color:orange;text-align:center;font-family:Courier">Render HTML</h1>"""
displayHTML(html)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create documentation cells
// MAGIC Render cell as <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">Markdown</a> using magic command: `%md`

// COMMAND ----------

// MAGIC %md
// MAGIC # Heading 1
// MAGIC ### Heading 3
// MAGIC > block quote
// MAGIC 
// MAGIC 1. **bold**
// MAGIC 2. *italicized*
// MAGIC 3. ~~strikethrough~~
// MAGIC ---
// MAGIC - [link](https://www.markdownguide.org/cheat-sheet/)
// MAGIC - `code`
// MAGIC 
// MAGIC ```
// MAGIC {
// MAGIC   "message": "This is a code block",
// MAGIC   "method": "https://www.markdownguide.org/extended-syntax/#fenced-code-blocks",
// MAGIC   "alternative": "https://www.markdownguide.org/basic-syntax/#code-blocks"
// MAGIC }
// MAGIC ```
// MAGIC 
// MAGIC ![Spark Logo](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
// MAGIC 
// MAGIC | Element         | Markdown Syntax |
// MAGIC |-----------------|-----------------|
// MAGIC | Heading         | `#H1` `##H2` `###H3` `#### H4` `##### H5` `###### H6` |
// MAGIC | Block quote     | `> blockquote` |
// MAGIC | Bold            | `**bold**` |
// MAGIC | Italic          | `*italicized*` |
// MAGIC | Strikethrough   | `~~strikethrough~~` |
// MAGIC | Horizontal Rule | `---` |
// MAGIC | Code            | ``` `code` ``` |
// MAGIC | Link            | `[text](https://www.example.com)` |
// MAGIC | Image           | `[alt text](image.jpg)`|
// MAGIC | Ordered List    | `1. First items` <br> `2. Second Item` <br> `3. Third Item` |
// MAGIC | Unordered List  | `- First items` <br> `- Second Item` <br> `- Third Item` |
// MAGIC | Code Block      | ```` ``` ```` <br> `code block` <br> ```` ``` ````|
// MAGIC | Table           |<code> &#124; col &#124; col &#124; col &#124; </code> <br> <code> &#124;---&#124;---&#124;---&#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br>|

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Access DBFS (Databricks File System)
// MAGIC Run file system commands on DBFS using magic command: `%fs`

// COMMAND ----------

// MAGIC %fs ls

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/README.md

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

// MAGIC %md
// MAGIC `%fs` is shorthand for the <a href="https://docs.databricks.com/dev-tools/databricks-utils.html" target="_blank">DBUtils</a> module: `dbutils.fs`

// COMMAND ----------

// MAGIC %fs help

// COMMAND ----------

// MAGIC %md
// MAGIC Run file system commands on DBFS using DBUtils directly

// COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

// COMMAND ----------

// MAGIC %md
// MAGIC Visualize results in a table using the Databricks <a href="https://docs.databricks.com/notebooks/visualizations/index.html#display-function-1" target="_blank">display</a> function

// COMMAND ----------

val files = dbutils.fs.ls("/databricks-datasets")
display(files)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create table
// MAGIC Run <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/index.html#sql-reference" target="_blank">Databricks SQL Commands</a> to create a table named `events` using BedBricks event files on DBFS.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "/mnt/training/ecommerce/events/events.parquet");

// COMMAND ----------

// MAGIC %md
// MAGIC This table was saved in the database created for you in classroom setup. See database name printed below.

// COMMAND ----------

println(databaseName)

// COMMAND ----------

// MAGIC %md
// MAGIC View your database and table in the Data tab of the UI.

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Query table and plot results
// MAGIC Use SQL to query `events` table

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM events

// COMMAND ----------

// MAGIC %md
// MAGIC Run the query below and then <a href="https://docs.databricks.com/notebooks/visualizations/index.html#plot-types" target="_blank">plot</a> results by selecting the bar chart icon

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT traffic_source, SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue
// MAGIC FROM events
// MAGIC GROUP BY traffic_source

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Add notebook parameters with widgets
// MAGIC Use <a href="https://docs.databricks.com/notebooks/widgets.html" target="_blank">widgets</a> to add input parameters to your notebook.
// MAGIC 
// MAGIC Create a text input widget using SQL.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE WIDGET TEXT state DEFAULT "CA"

// COMMAND ----------

// MAGIC %md
// MAGIC Access the current value of the widget using the function `getArgument`

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM events
// MAGIC WHERE geo.state = getArgument("state")

// COMMAND ----------

// MAGIC %md
// MAGIC Remove the text widget

// COMMAND ----------

// MAGIC %sql
// MAGIC REMOVE WIDGET state

// COMMAND ----------

// MAGIC %md
// MAGIC To create widgets in Python, Scala, and R, use the DBUtils module: `dbutils.widgets`

// COMMAND ----------

dbutils.widgets.text("name", "Brickster", "Name")
dbutils.widgets.multiselect("colors", "orange", Seq("red", "orange", "black", "blue"), "Traffic Sources")

// COMMAND ----------

// MAGIC %md
// MAGIC Access the current value of the widget using the `dbutils.widgets` function `get`

// COMMAND ----------

val name = dbutils.widgets.get("name")
val colors = dbutils.widgets.get("colors").split(",")

var html = "<div>Hi %s! Select your color preference.</div>".format(name)
for (c <- colors) {
  html += """<label for="%s" style="color:%s"><input type="radio"> %s</label><br>""".format(c, c, c)
}    

displayHTML(html)

// COMMAND ----------

// MAGIC %md
// MAGIC Remove all widgets

// COMMAND ----------

dbutils.widgets.removeAll()

// COMMAND ----------

// MAGIC %md
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) BedBricks Datasets Lab
// MAGIC Explore BedBricks datasets
// MAGIC 1. View data files in DBFS using magic commands
// MAGIC 1. View data files in DBFS using dbutils
// MAGIC 1. Create tables from files in DBFS
// MAGIC 1. Execute SQL to answer questions on BedBricks datasets

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### 1. View data files in DBFS using magic commands
// MAGIC Use a magic command to display files located in the DBFS directory: **`/mnt/training/ecommerce`**
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You should see four items: `events`, `products`, `sales`, `users`

// COMMAND ----------

// TODO

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### 2. View data files in DBFS using dbutils
// MAGIC - Use **`dbutils`** to get the files at the directory above and save it to the variable **`files`**
// MAGIC - Use the Databricks display() function to display the contents in **`files`**
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You should see four items: `events`, `items`, `sales`, `users`

// COMMAND ----------

// TODO
val files = dbutils.FILL_IN
display(files)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Create tables below from files in DBFS
// MAGIC - Create `users` table using files at location `"/mnt/training/ecommerce/users/users.parquet"` 
// MAGIC - Create `sales` table using files at location `"/mnt/training/ecommerce/sales/sales.parquet"` 
// MAGIC - Create `products` table using files at location `"/mnt/training/ecommerce/products/products.parquet"` 
// MAGIC 
// MAGIC (The `events` table was created above using files at location `"/mnt/training/ecommerce/events/events.parquet"`)

// COMMAND ----------

// TODO

// COMMAND ----------

// MAGIC %md
// MAGIC Use the data tab of the workspace UI to confirm your tables were created.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Execute SQL to answer questions on BedBricks datasets

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### What products are available for purchase at BedBricks?
// MAGIC Execute a SQL query that selects all from the **`products`** table
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You should see 12 products.

// COMMAND ----------

// TODO

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### What is the average purchase revenue for a transaction at BedBricks?
// MAGIC Execute a SQL query that computes the average **`purchase_revenue_in_usd`** from the **`sales`** table
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** The result should be `1042.79`

// COMMAND ----------

// TODO

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##### What types of events are recorded on the BedBricks website?
// MAGIC Execute a SQL query that selects distinct values in **`event_name`** from the **`events`** table
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You should see 23 distinct `event_name` values

// COMMAND ----------

// TODO

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
