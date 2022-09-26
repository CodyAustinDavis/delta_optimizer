# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebook uses the query history API on Databricks SQL to pull the query history for all users within the last X days and builds a SQL profile from the query text to find most key columns to ZORDER on
# MAGIC 
# MAGIC 
# MAGIC ### RETURNS
# MAGIC 
# MAGIC 1. delta_optimizer.query_column_statistics - Column level query stats
# MAGIC 2. delta_optimizer.query_summary_statistics - Query level query stats
# MAGIC 3. delta_optimizer.raw_query_history_statistics - Raw Query History Stats
# MAGIC 
# MAGIC ### Depedencies
# MAGIC <li> https://github.com/macbre/sql-metadata -- pip install sql-metadata (installed from requirements.txt)
# MAGIC <li> Ensure that you either get a token as a secret or use a cluster with the env variable called DBX_TOKEN to authenticate to DBSQL
# MAGIC 
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC DBX_TOKEN = os.environ.get("DBX_TOKEN")

# COMMAND ----------

# MAGIC %pip install -r deltaoptimizer/requirements.txt

# COMMAND ----------

from deltaoptimizer.deltaoptimizer import DeltaOptimizerBase, DeltaProfiler, QueryProfiler, DeltaOptimizer

# COMMAND ----------

import os
DBX_TOKEN = os.environ.get("DBX_TOKEN")

# COMMAND ----------

# DBTITLE 1,Set up params before running
## Assume running in a Databricks notebook
dbutils.widgets.dropdown("Query History Lookback Period (days)", defaultValue="3",choices=["1","3","7","14","30","60","90"])
dbutils.widgets.text("SQL Warehouse Ids (csv list)", "")
dbutils.widgets.text("Workspace DNS:", "")
dbutils.widgets.text("Database Names (csv):", "")
dbutils.widgets.dropdown("optimizeMethod", "Both", ["Reads", "Writes", "Both"])

# COMMAND ----------

# DBTITLE 1,Run Delta Optimizer


lookbackPeriod = int(dbutils.widgets.get("Query History Lookback Period (days)"))
warehouseIdsList = [i.strip() for i in dbutils.widgets.get("SQL Warehouse Ids (csv list)").split(",")]
workspaceName = dbutils.widgets.get("Workspace DNS:").strip()
warehouse_ids = dbutils.widgets.get("SQL Warehouse Ids (csv list)")
print(f"Loading Query Profile to delta from workspace: {workspaceName} from Warehouse Ids: {warehouseIdsList} for the last {lookbackPeriod} days...")

#### Step 1: Build Profile
## Initialize Profiler
query_profiler = QueryProfiler(workspaceName, warehouseIdsList)


query_profiler.build_query_history_profile( dbx_token = DBX_TOKEN, mode='auto', lookback_period_days=lookbackPeriod)


##### Step 2: Build stats from transaction logs/table data

## Assume running on Databricks notebooks if not imported
databases_raw = dbutils.widgets.get("Database Names (csv):")


## Initialize class and pass in database csv string
profiler = DeltaProfiler(monitored_db_csv='default') ## examples include 'default', 'mydb1,mydb2', 'all' or leave blank

## Get tables
profiler.get_all_tables_to_monitor()

## Get predicate analysis for tables
profiler.parse_stats_for_tables()

## Build final table output
profiler.build_all_tables_stats()

## Generate cardinality stats
profiler.build_cardinality_stats()


####### Step 3: Build Strategy and Rank
optimize_method = dbutils.widgets.get("optimizeMethod")

## Build Strategy
delta_optimizer = DeltaOptimizer()

delta_optimizer.build_optimization_strategy()

