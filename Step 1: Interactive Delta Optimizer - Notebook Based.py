# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebook imports the Delta Optimizer and does the following: 
# MAGIC 
# MAGIC <ul> 
# MAGIC   
# MAGIC   <li> Poll Query History API and get List of Queries for a set of SQL Warehouses
# MAGIC   <li> Analyze transaction logs for tables in set of databases (all by default) -- file size, partitions, merge predicates
# MAGIC   <li> Rank unified strategy
# MAGIC     
# MAGIC </ul>
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

from deltaoptimizer.deltaoptimizer import DeltaProfiler, QueryProfiler, DeltaOptimizer

# COMMAND ----------

# DBTITLE 1,Register and Retrieve DBX Auth Token
import os
DBX_TOKEN = os.environ.get("DBX_TOKEN")

# COMMAND ----------

# DBTITLE 1,Set up params before running
## Assume running in a Databricks notebook
dbutils.widgets.dropdown("Query History Lookback Period (days)", defaultValue="3",choices=["1","3","7","14","30","60","90"])
dbutils.widgets.text("SQL Warehouse Ids (csv list)", "")
dbutils.widgets.text("Workspace DNS:", "")
dbutils.widgets.text("Database Names (csv):", "")
dbutils.widgets.dropdown("optimize_method", "both", ["read", "write", "both"])

# COMMAND ----------

# DBTITLE 1,Run Delta Optimizer - Input above params first
lookbackPeriod = int(dbutils.widgets.get("Query History Lookback Period (days)"))
warehouseIdsList = [i.strip() for i in dbutils.widgets.get("SQL Warehouse Ids (csv list)").split(",")]
workspaceName = dbutils.widgets.get("Workspace DNS:").strip()
warehouse_ids = dbutils.widgets.get("SQL Warehouse Ids (csv list)")

####### Step 1: Build Profile #######
## Initialize Profiler
#query_profiler = QueryProfiler(workspaceName, warehouseIdsList)


#query_profiler.build_query_history_profile( dbx_token = DBX_TOKEN, mode='auto', lookback_period_days=lookbackPeriod)


####### Step 2: Build stats from transaction logs/table data #######

## Assume running on Databricks notebooks if not imported
databases_raw = dbutils.widgets.get("Database Names (csv):")


## Initialize class and pass in database csv string
profiler = DeltaProfiler( monitored_db_csv= databases_raw) ## examples include 'default', 'mydb1,mydb2', 'all' or leave blank

## Get tables
profiler.get_all_tables_to_monitor()

## Get predicate analysis for tables
profiler.parse_stats_for_tables()

## Build final table output
profiler.build_all_tables_stats()

## Generate cardinality stats
profiler.build_cardinality_stats()


####### Step 3: Build Strategy and Rank #######
optimize_method = dbutils.widgets.get("optimize_method")

## Build Strategy
delta_optimizer = DeltaOptimizer()

delta_optimizer.build_optimization_strategy()


# COMMAND ----------

# DBTITLE 1,Return most up to date results!
df = delta_optimizer.get_results()

# COMMAND ----------

df.display()
