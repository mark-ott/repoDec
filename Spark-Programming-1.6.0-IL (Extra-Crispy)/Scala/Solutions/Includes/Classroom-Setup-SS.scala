// Databricks notebook source
// MAGIC 
// MAGIC %run ./Classroom-Setup

// COMMAND ----------

val couponsCheckpointPath = workingDir + "/coupon-sales/checkpoint"
val couponsOutputPath = workingDir + "/coupon-sales/output"

val deltaEventsPath = workingDir + "/delta/events"
val deltaSalesPath = workingDir + "/delta/sales"
val deltaUsersPath = workingDir + "/delta/users"

displayHTML("")

