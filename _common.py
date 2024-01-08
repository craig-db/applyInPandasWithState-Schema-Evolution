# Databricks notebook source
my_name = spark.sql("select current_user()").collect()[0][0]
my_name = my_name[:my_name.rfind('@')].replace(".", "_")

# COMMAND ----------

dbutils.widgets.dropdown("clean_up", "No", ["Yes", "No"], label="Clean up tables and checkpoint")

# COMMAND ----------

dbutils.widgets.text("bronze_table", f"{my_name}.bronze_events_202401", "Source table")
dbutils.widgets.text("silver_table", f"{my_name}.silver_aggregates_202401", "Streaming Sink")

# COMMAND ----------

dbutils.widgets.text("checkpoint_root", f"dbfs:/tmp/silver_aggregates_202401_{my_name}/checkpoints", "Checkpoint root")

# COMMAND ----------

# Date to start
dbutils.widgets.text("bronze_start_date", "20200101", "Events start on (YYYYMMDD)")
dbutils.widgets.text("bronze_num_keys", "10", "Num. unique keys")
dbutils.widgets.text("bronze_num_statevalue_columns", "10", "Num. state value parts")
dbutils.widgets.text("bronze_num_other_columns", "10", "Num. other cols")
dbutils.widgets.text("bronze_days_to_populate", "5", "Sim days")
dbutils.widgets.text("bronze_session_max_hours", "4", "Session max hours")

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
checkpoint_root = dbutils.widgets.get("checkpoint_root")
clean_up = dbutils.widgets.get("clean_up")

bronze_start_date = int(dbutils.widgets.get("bronze_start_date"))
bronze_num_keys = int(dbutils.widgets.get("bronze_num_keys"))
bronze_num_statevalue_columns = int(dbutils.widgets.get("bronze_num_statevalue_columns"))
bronze_num_other_columns = int(dbutils.widgets.get("bronze_num_other_columns"))
bronze_days_to_populate = int(dbutils.widgets.get("bronze_days_to_populate"))
bronze_session_max_hours = int(dbutils.widgets.get("bronze_session_max_hours"))

# COMMAND ----------

import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Iterator, List, Tuple
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import pandas as pd

# COMMAND ----------

def add_one_hour_to_date(dt_obj):
    return dt_obj + datetime.timedelta(hours=1)
    
def start_at(num_date):
    date_str = str(num_date * 100).zfill(10)
    # Convert string to datetime object
    return datetime.datetime.strptime(date_str, '%Y%m%d%H')

def end_at(dt_date, num_days):
    return dt_date + datetime.timedelta(days=num_days)

# COMMAND ----------

parameters = [
  "bronze_table", "silver_table", "checkpoint_root", "bronze_start_date", "bronze_num_keys", 
  "bronze_num_statevalue_columns", "bronze_num_other_columns", "bronze_days_to_populate", 
  "bronze_session_max_hours", "clean_up"
]

print("Parameters for the run:")
for w in parameters:
  print(f"  {w}: {eval(w)}")

# COMMAND ----------

# MAGIC %md
# MAGIC
