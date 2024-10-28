# Databricks notebook source
# MAGIC %run "./_common"

# COMMAND ----------

df = spark.read.format("state-metadata").load(checkpoint_root)
df.createOrReplaceTempView("state_metadata")

# COMMAND ----------


