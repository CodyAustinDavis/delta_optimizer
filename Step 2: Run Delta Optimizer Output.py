# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Run the output of recommended optimize statements as a single run or schedule as a periodic job
# MAGIC 
# MAGIC <h4> Run this after the delta optimizer is finished </h4>
# MAGIC 
# MAGIC #### Roadmap: 
# MAGIC 
# MAGIC 1. Use DLT to auto optimize LIVE and Normal Delta Tables if possible
# MAGIC 2. Use DLT metaprogramming framework to run in parallel (performance implications)
# MAGIC 3. Use Jobs API to automatically set up a daily / hourly job for this. This is NOT always recommended by default. The optimize timing greatly depends on the ETL pipelines
# MAGIC 4. Dyanmically decide how often to run ANALYZE TABLE commands based on table size mapping (job that does this for you)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Run Commands in Particular Order:
# MAGIC 
# MAGIC <li> 1. ALTER TABLE
# MAGIC <li> 2. Column Reordering
# MAGIC <li> 3. OPTIMIZE TABLE
# MAGIC <li> 4. ANALYZE TABLE

# COMMAND ----------

# MAGIC %pip install -r deltaoptimizer/requirements.txt

# COMMAND ----------

from deltaoptimizer.deltaoptimizer import DeltaOptimizerBase, DeltaProfiler, QueryProfiler, DeltaOptimizer

# COMMAND ----------

delta_optimizer = DeltaOptimizer()

# COMMAND ----------

## This table by default has only 1 file, so it shouldnt be expensive to collect
config_row = delta_optimizer.get_results().collect()

# COMMAND ----------

# DBTITLE 1,Step 1 - Get Table Properties Config
config_tbl_prop = [i[3] for i in config_row]

print(f"Running {len(config_tbl_prop)} TBL PROPERTIES (file size and re-writes) commands: \n {config_tbl_prop}")

# COMMAND ----------

# DBTITLE 1,Run TBL Properties Commands
for i in config_tbl_prop:
    try: 
        print(f"Running TABLE PROPERTIES command for {i}...")
        spark.sql(i)
        print(f"Completed TABLE PROPERTIES command for {i}!\n")
        
    except Exception as e:
        print(f"TABLE PROPERTIES failed with error: {str(e)}\n")

# COMMAND ----------

# DBTITLE 1,Move Z-Order columns to front
col_list = config_tbl_prop = [i[5] for i in config_row]

### This is a recursive step, ordering needs to happend one at a time
## Starting simple, just moving ZORDEr cols to front, but this can become more nuanced
for i in col_list:
  for j in i:
    try: 
      spark.sql(j)
      
    except Exception as e:
      print(f"Unable to change order (usually means cause its an Id column and doesnt need reordering anyways...skipping to next columns) \n with error: {str(e)} \n ")
      

# COMMAND ----------

# DBTITLE 1,Step 2 - Get config for OPTIMIZE Commands
## This table by default has only 1 file, so it shouldnt be expensive to collect
config_optim = [i[2] for i in config_row]

print(f"Running {len(config_optim)} OPTIMIZE commands: \n {config_optim}")

# COMMAND ----------

# DBTITLE 1,Run through OPTIMIZE commands
for i in config_optim:
    try: 
        print(f"Running OPTIMIZE command for {i}...")
        spark.sql(i)
        print(f"Completed OPTIMIZE command for {i}!\n ")
        
    except Exception as e:
        print(f"Optimize failed with error: {str(e)}\n")


# COMMAND ----------

# DBTITLE 1,Step 3 - Get Config for ANALYZE TABLE commands
## This table by default has only 1 file, so it shouldnt be expensive to collect
config_tbl_stats = [i[4] for i in config_row]

print(f"Running {len(config_tbl_stats)} TBL PROPERTIES (file size and re-writes) commands: \n {config_tbl_stats}")

# COMMAND ----------

# DBTITLE 1,Run through Config for ANALYZE
for i in config_tbl_stats:
    try: 
        print(f"Running ANALYZE TABLE command for {i}...")
        spark.sql(i)
        print(f"Completed ANALYZE TABLE command for {i}!\n")
        
    except Exception as e:
        print(f"ANALYZE TABLE failed with error: {str(e)}\n")


# COMMAND ----------


