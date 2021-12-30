// Databricks notebook source

//*******************************************
// TAG API FUNCTIONS
//*******************************************

// Get all tags
def getTags(): Map[com.databricks.logging.TagDefinition,String] = {
  com.databricks.logging.AttributionContext.current.tags
}

// Get a single tag's value
def getTag(tagName: String, defaultValue: String = null): String = {
  val values = getTags().collect({ case (t, v) if t.name == tagName => v }).toSeq
  values.size match {
    case 0 => defaultValue
    case _ => values.head.toString
  }
}

//*******************************************
// USER, USERNAME, AND USERHOME FUNCTIONS
//*******************************************

// Get the user's username
def getUsername(): String = {
  return try {
    dbutils.widgets.get("databricksUsername")
  } catch {
    case _: Exception => getTag("user", java.util.UUID.randomUUID.toString.replace("-", ""))
  }
}

// Get the user's userhome
def getUserhome(): String = {
  val username = getUsername()
  return s"dbfs:/user/$username"
}

def getModuleName(): String = {
  // This will/should fail if module-name is not defined in the Classroom-Setup notebook
  return spark.conf.get("com.databricks.training.module-name")
}

def getLessonName(): String = {
  // If not specified, use the notebook's name.
  return dbutils.notebook.getContext.notebookPath.get.split("/").last
}

def getWorkingDir(): String = {
  val langType = "s" // for scala
  val lessonName = getLessonName().replaceAll("[^a-zA-Z0-9]", "_")
  val moduleName = getModuleName().replaceAll("[^a-zA-Z0-9]", "_")
  val userhome = getUserhome()
  return f"${userhome}/${moduleName}/${lessonName}/${langType}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").toLowerCase()
}

//**********************************
// USER DATABASE FUNCTIONS
//**********************************

def getDatabaseName(username:String, moduleName:String, lessonName:String):String = {
  val user = username.replaceAll("[^a-zA-Z0-9]", "")
  val module = moduleName.replaceAll("[^a-zA-Z0-9]", "_")
  val lesson = lessonName.replaceAll("[^a-zA-Z0-9]", "_")
  val langType = "scala" // for scala
  val databaseName = f"${user}_${module}_${lesson}_${langType}".toLowerCase
  return databaseName
}

// Create a user-specific database
def createUserDatabase(username:String, moduleName:String, lessonName:String):String = {
  val databaseName = getDatabaseName(username, moduleName, lessonName)

  spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(databaseName))
  spark.sql("USE %s".format(databaseName))

  return databaseName
}

// ****************************************************************************
// Utility method to determine whether a path exists
// ****************************************************************************

def pathExists(path:String):Boolean = {
  try {
    dbutils.fs.ls(path)
    return true
  } catch{
    case e: Exception => return false
  }
}

// ****************************************************************************
// Utility method for recursive deletes
// Note: dbutils.fs.rm() does not appear to be truely recursive
// ****************************************************************************

def deletePath(path:String):Unit = {
  val files = dbutils.fs.ls(path)

  for (file <- files) {
    val deleted = dbutils.fs.rm(file.path, true)

    if (deleted == false) {
      if (file.isDir) {
        deletePath(file.path)
      } else {
        throw new java.io.IOException("Unable to delete file: " + file.path)
      }
    }
  }

  if (dbutils.fs.rm(path, true) == false) {
    throw new java.io.IOException("Unable to delete directory: " + path)
  }
}

// ****************************************************************************
// Utility method to clean up the workspace at the end of a lesson
// ****************************************************************************

def classroomCleanup(username:String, moduleName:String, lessonName:String, dropDatabase: Boolean):Unit = {

  // Stop any active streams
  for (stream <- spark.streams.active) {
    stream.stop()

    // Wait for the stream to stop
    var queries = spark.streams.active.filter(_.name == stream.name)

    while (queries.length > 0) {
      Thread.sleep(5*1000) // Give it a couple of seconds
      queries = spark.streams.active.filter(_.name == stream.name)
    }
  }

  // Drop the tables only from specified database
  val database = getDatabaseName(username, moduleName, lessonName)
  try {
    val tables = spark.sql(s"show tables from $database").select("tableName").collect()
    for (row <- tables){
      var tableName = row.getAs[String]("tableName")
      spark.sql("drop table if exists %s.%s".format(database, tableName))

      // In some rare cases the files don't actually get removed.
      Thread.sleep(1000) // Give it just a second...
      val hivePath = "dbfs:/user/hive/warehouse/%s.db/%s".format(database, tableName)
      dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure

    }
  } catch {
    case _: Exception => () // ignored
  }

  // Remove files created from previous runs
  val path = getWorkingDir()
  if (pathExists(path)) {
    deletePath(path)
  }

  // Drop the database if instructed to
  if (dropDatabase){
    spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")

    // In some rare cases the files don't actually get removed.
    Thread.sleep(1000) // Give it just a second...
    val hivePath = "dbfs:/user/hive/warehouse/%s.db".format(database)
    dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure

    displayHTML("Dropped database and removed files in working directory")
  }
}

// ****************************************************************************
// Utility method to delete a database
// ****************************************************************************

def deleteTables(database:String):Unit = {
  spark.sql("DROP DATABASE IF EXISTS %s CASCADE".format(database))
}

// ****************************************************************************
// Placeholder variables for coding challenge type specification
// ****************************************************************************
object FILL_IN {
  val VALUE = null
  val ARRAY = Array(Row())
  val SCHEMA = org.apache.spark.sql.types.StructType(List())
  val ROW = Row()
  val LONG: Long = 0
  val INT: Int = 0
  def DATAFRAME = spark.emptyDataFrame
  def DATASET = spark.createDataset(Seq(""))
}

//**********************************
// Set up student environment
//**********************************

val moduleName = getModuleName()
val lessonName = getLessonName()
val username = getUsername()
val userhome = getUserhome()
val workingDir = getWorkingDir()
val databaseName = createUserDatabase(username, moduleName, lessonName)

classroomCleanup(username, moduleName, lessonName, false)

displayHTML("")

