# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce fake data
# MAGIC This notebook creates fake data and populates a bronze table. The <a target=_new href="$./_common">_common</a> notebook includes a number of variables used within this notebook and the <a target=_new href="$./Bronze to Silver">Bronze to Silver</a> notebook. Review it to understand some of the variables used in this demo. In the next cell, the `_common` notebook is run with some parameters that you may want to adjust.
# MAGIC
# MAGIC #### Clean up
# MAGIC To clean up the tables and checkpoint used in this demo, change the value of `clean_up` to `Yes` in the cell below.

# COMMAND ----------

# DBTITLE 1,Set common variables
# MAGIC %run "./_common" $bronze_num_statevalue_columns="3" $bronze_num_other_columns="4" $clean_up="No"

# COMMAND ----------

# DBTITLE 1,Clean up tables and checkpoint
if clean_up == "Yes":
  print("Cleaning up tables and checkpoint directory")
  spark.sql(f"drop table if exists {bronze_table}")
  spark.sql(f"drop table if exists {silver_table}")
  dbutils.fs.rm(checkpoint_root, True)
  dbutils.notebook.exit("Cleanup is stopping the notebook execution now")

# COMMAND ----------

# DBTITLE 1,Setup starting event_timestamp and starting key
# Event timestamps will begin at this data
dt_start = start_at(bronze_start_date)
start_range = 0
end_range = bronze_num_keys
num_hours_processed = 0
print(f"Events will start with an event_timestamp of {dt_start} and will run for {bronze_days_to_populate} simulated days where, for {bronze_session_max_hours} hours, {bronze_session_max_hours} unique keys will have an event generated for it every hour.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### For additional simulated data, Run all below this cell (after uncommenting/editing the first cell)
# MAGIC Subsequent runs will add an addition column to the source table that is used in the state value schema. After a subsequent run of the code below, if you stop and restart the consumer stream (the <a target=_new href="$./Bronze to Silver">Bronze to Silver</a> notebook, you can expect it to fail!
# MAGIC

# COMMAND ----------

# DBTITLE 1,Set event_timestamp to stop generating data
dt_end = end_at(dt_start, bronze_days_to_populate)
print(f"Data will be generated from {dt_start} to {dt_end} every (simulated) hour for {bronze_num_keys} keys")

# COMMAND ----------

# DBTITLE 1,Generate data
while dt_end > dt_start:
  dt_start =  add_one_hour_to_date(dt_start)
  df = spark.range(start_range, end_range).withColumn("event_timestamp", lit(dt_start))
  for i in range(bronze_num_statevalue_columns):
    df = df.withColumn(f"statevalue_{i}", lit(1))
  for i in range(bronze_num_other_columns):
    df = df.withColumn(f"other_column_{i}", lit(1))
  df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(f"{bronze_table}")
  delta_version = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
  print(f"Delta Version: {delta_version}, Starting: {dt_start}, Ending: {dt_end}, " + \
    f"Start key: {start_range}, End key: {end_range}")

  num_hours_processed += 1
  if num_hours_processed % bronze_session_max_hours == 0:
    start_range += bronze_num_keys
    end_range += bronze_num_keys    

# COMMAND ----------

# DBTITLE 1,How many state columns this run?
bronze_num_statevalue_columns

# COMMAND ----------

# DBTITLE 1,How many state columns for the next run?
bronze_num_statevalue_columns += 1
bronze_num_statevalue_columns

# COMMAND ----------

bronze_num_other_columns += 1

# COMMAND ----------


