# Databricks notebook source
# DBTITLE 1,Set common variables
# MAGIC %run "./_common"

# COMMAND ----------

# DBTITLE 1,Create wdigets for notebook-specific variables
dbutils.widgets.text("checkpoint_iteration", "0", "State Version")
dbutils.widgets.text("safety_run_duration", "0.25", "How long to run (in hours) using MERGE")
dbutils.widgets.text("starting_version", "0", "'Start at' Delta version of source")
dbutils.widgets.text("maxFilesPerTrigger", "1", "Max input files per trigger")
dbutils.widgets.dropdown("debug_mode", "True", ["True", "False"], "Debug Mode")

# COMMAND ----------

# DBTITLE 1,Set variables
checkpoint_iteration = int(dbutils.widgets.get("checkpoint_iteration"))
safety_run_duration = float(dbutils.widgets.get("safety_run_duration"))
starting_version = int(dbutils.widgets.get("starting_version"))
maxFilesPerTrigger = int(dbutils.widgets.get("maxFilesPerTrigger"))
debug_mode = bool(dbutils.widgets.get("debug_mode") == "True")

# COMMAND ----------

# DBTITLE 1,Show variables
print(f"""
checkpoint_iteration: {checkpoint_iteration}
safety_run_duration: {safety_run_duration}
starting_version: {starting_version}
maxFilesPerTrigger: {maxFilesPerTrigger}
debug_mode: {debug_mode}
""")

# COMMAND ----------

# DBTITLE 1,Establish DataFrame from source Delta table
df = spark.readStream.option("maxFilesPerTrigger", maxFilesPerTrigger).option("startingVersion", starting_version).table(f"{bronze_table}")

# COMMAND ----------

# DBTITLE 1,Establish which columns are for aggregating, for state and for output
# Output will include two timestamps, the earliest one and the latest (which will be in event_timestamp)
output_fields = df.schema.fields + [StructField("earliest_timestamp", TimestampType(), False)]
output_schema = StructType(output_fields)
output_columns = [field for field in output_schema.fieldNames()]

# state schema does not need to key (id)
state_schema = StructType([field for field in output_schema.fields if field.name != "id"])
state_schema_columns = [field for field in state_schema.fieldNames()]

state_columns = [field.name for field in state_schema.fields if ("statevalue" in field.name.lower())]
non_state_columns = [field.name for field in state_schema.fields if not ("statevalue" in field.name.lower())]

# COMMAND ----------

output_columns

# COMMAND ----------

# columns we will keep adding the incoming values to
state_columns

# COMMAND ----------

# columns we'll keep only the latest (with the exception of earliest_timestamp which will be the first event_timestamp for the key)
non_state_columns

# COMMAND ----------

state_schema_columns

# COMMAND ----------

# DBTITLE 1,Reconstitute a Pandas DataFrame from the stored tuple from the state store
# Recreate a Pandas DataFrame given the tuple stored in state
def df_from_state(state: GroupState):
    if debug_mode: print("In df_from_state")
    prev_state = state.getOption
    stored = None
    if prev_state:
        if debug_mode: print(f"Retrieved state: {prev_state}")
        stored = pd.DataFrame([prev_state], columns=state_schema_columns)
        if debug_mode: print(f"Reconstituted DataFrame from state: {stored.to_string()}")
    return stored

# COMMAND ----------

# DBTITLE 1,Aggregate rows
def consolidate_df(df: pd.DataFrame, cols_to_agg: List[str]):
    # Initialize a dictionary to store the consolidated data
    consolidated_data = {}

    # Sum the values for 'state_n' columns
    for col in cols_to_agg:
        consolidated_data[col] = df[col].sum()

    # Retain the latest values for 'other_n' columns
    other_columns = [col for col in df if col.startswith("other_")]
    latest_row = df.sort_values(by='event_timestamp', ascending=False).iloc[0]
    for col in other_columns:
        consolidated_data[col] = latest_row[col]

    # Newest and earliest event timestamp
    consolidated_data['event_timestamp'] = df['event_timestamp'].max()
    consolidated_data['earliest_timestamp'] = df['event_timestamp'].min()

    # Return the consolidated DataFrame
    return pd.DataFrame([consolidated_data])

# COMMAND ----------

# DBTITLE 1,Aggregate Pandas DataFrames
def consolidate_dfs(pd_dfs: Iterator[pd.DataFrame], cols_to_agg: List[str]):
    if debug_mode: print("In consolidate_dfs")
    # Initialize an empty DataFrame to store the consolidated results
    consolidated_result = pd.DataFrame()

    for df in pd_dfs:
        # Consolidate each DataFrame using the previously defined function
        consolidated_df = consolidate_df(df, cols_to_agg)

        # Append the consolidated DataFrame to the result
        consolidated_result = pd.concat([consolidated_result, consolidated_df], ignore_index=True)

    return consolidate_df(consolidated_result, cols_to_agg)

# COMMAND ----------

# DBTITLE 1,The applyInPandasWithState function
import itertools

def func(
    key: Tuple[int], pdfs: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:

    # So you can see, in the logs, what is happening along the execution path
    if debug_mode: 
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

    (session_id,) = key

    prev_state = df_from_state(state)
    if state.hasTimedOut:
        if debug_mode: print(f"Timeout for: {session_id}")
        state.remove()
        prev_state["id"] = session_id
        prev_state.reindex(columns=output_columns)
        if debug_mode: print(f"DataFrame to emit during TimedOut: {prev_state.to_string()}")

        yield prev_state
    else:
        if debug_mode: print(f"session_id: {session_id}")
        
        accumulated_df = consolidate_dfs(pdfs, state_columns)
        if debug_mode: print(f"accumulated_df: {accumulated_df.to_string()}")

        if prev_state is not None:
            accumulated_df = consolidate_dfs(itertools.chain([prev_state, accumulated_df]), state_columns)
            if debug_mode: print(f"accumulated_df (after adding state): {accumulated_df.to_string()}")

        joined_df = accumulated_df.reindex(columns=state_schema_columns)
        if debug_mode: print(f"DataFrame to build tuple: {joined_df.to_string()}")
        tuple_to_save = tuple(joined_df.iloc[0])
        if debug_mode: print(f"The tuple_to_save: {tuple_to_save}")
        
        # Save the updated state to the state store
        state.update(tuple_to_save)
        cur_state_watermark = state.getCurrentWatermarkMs()
        if cur_state_watermark != 0:
            state.setTimeoutTimestamp(cur_state_watermark + (bronze_session_max_hours * 60 * 60 * 1000))
            if debug_mode: print(f"added {bronze_session_max_hours} hours to current watermark of {cur_state_watermark}")
        if debug_mode: print(f"tuple saved to state for key {session_id}")

        yield pd.DataFrame()     

# COMMAND ----------

df_wm = df.withWatermark("event_timestamp", f"{bronze_session_max_hours} hours")

# COMMAND ----------

out_df = df_wm.groupBy(df["id"]).applyInPandasWithState(
  func,
  output_schema,
  state_schema,
  "append",
  GroupStateTimeout.EventTimeTimeout
).withColumn("emit_timestamp", current_timestamp())

# COMMAND ----------

# DBTITLE 1,View output of applyInPandasWithState
if debug_mode: display(out_df)

# COMMAND ----------

# DBTITLE 1,Ensure our MERGE target (Delta table) has an evolving schema
spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

# DBTITLE 1,Set a "safety" duration after which the stream will switch from MERGE to append
import datetime
ts_start = datetime.datetime.now()
dt_end = ts_start + datetime.timedelta(hours=safety_run_duration)

def safe_period_passed():
    # add wait_hrs to datetime object
    dt_now = datetime.datetime.now()
    # check if dt_end has passed
    if dt_now > dt_end:
        if debug_mode: print(f"{dt_now} > {dt_end} --> True")
        return True
    else:
        if debug_mode: print(f"{dt_now} > {dt_end} --> False")
        return False 

# COMMAND ----------

# DBTITLE 1,Create checkpoint dir
dbutils.fs.mkdirs(f"{checkpoint_root}/{checkpoint_iteration}")

# COMMAND ----------

# DBTITLE 1,The function used in applyInPandasWithState
def feb_func(batch_df: DataFrame, batch_id: int):
  if checkpoint_iteration < 1 or safe_period_passed():
    if debug_mode: print(f"Using append for batch {batch_id}")
    (batch_df
      .write
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable(f"{silver_table}")
    )
  else:
    if debug_mode: print(f"Using MERGE for batch {batch_id} until {dt_end}")
    
    # Set the dataframe to view name
    batch_df.createOrReplaceTempView("updates")

    # Use the view name to apply MERGE
    batch_df.sparkSession.sql(f"""
        MERGE INTO {silver_table} t 
        USING updates s 
        ON s.id = t.id 
        WHEN MATCHED THEN UPDATE SET * 
        WHEN NOT MATCHED THEN INSERT * 
    """)

# COMMAND ----------

# DBTITLE 1,Run the foreachBatch stream
# Write the output of a streaming aggregation query into Delta table
out_df.writeStream \
    .option("checkpointLocation", f"{checkpoint_root}/{checkpoint_iteration}") \
    .foreachBatch(feb_func) \
    .start()

# COMMAND ----------

# DBTITLE 1,You can periodically inspect the target/sink with this query
# display(spark.sql(f"select * from {silver_table} order by statevalue_0 asc, id desc"))

# COMMAND ----------


