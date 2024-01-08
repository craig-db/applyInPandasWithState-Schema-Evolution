# Databricks notebook source
# MAGIC %run "./_common" 

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's explore the bronze table, which is the simulated source event data

# COMMAND ----------

display(spark.sql(f"""

  select *
    from {bronze_table}
    order by id desc, event_timestamp desc

"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we want to check that only one row is returned for each key (the "id" column). This query will return row counts by id. We should only see one row per id (row_count)

# COMMAND ----------

display(spark.sql(f"""

with base as (
  select id, count(*) row_count
    from {silver_table}
    group by id
)
select *
  from base
order by row_count desc, id asc

"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's peek into the contents of the table

# COMMAND ----------

display(spark.sql(f"""

  select *
    from {silver_table}
    order by id desc

"""))

# COMMAND ----------


