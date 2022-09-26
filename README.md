# delta_optimizer
Automated Optimization System for a Delta-based Lakehouse running on Spark or Photon

%md

# Delta Optimizer

## Purpose:
<p1> The Delta optimizer scrapes and analyzes the query history in DBSQL via the Query History API, as well as the Delta transaction logs on one or many databases, builds a data profile to determine the most important columns that each tables should be Z-ordered by. This aims to drastically reduce the amount of manual discovery and tuning users must do to properly optimize their delta tables, especially when the primary query interface is through a DBSQL Warehouse (as an analyst using SQL or a BI tool that auto-generates SQL). This is especially key when BI tools primarily pass auto-generated SQL to a DBSQL Warehouse, thus making it much more difficult to optimize tables manually at scale. </p1>
  
  
### Steps: 

<li> 1. Gather Query History and calculate statistics on all columns for all tables (option to select a particular database)
<li> 2. Read transaction logs and find any merge predicates (if any) run for all tables in one or many databases
<li> 3. Calculate Statistics and Rank Columns for each table for Z-order strategy using runtime stats, occurence stats, and cardinality stats
<li> 4. Prepare and save a ready-to-use config delta table that can be ingested by a job or DLT to actually run the recommended OPTIMIZE/ANALYZE/TBLPROP commands </li>
  
### Roadmap: 

#### General Roadmap: 

<li> 1. Separate optimization rules from code logic to make rules configurable
<li> 2. Add option to run for user or simply provide a DBSQL Dashboard to make suggestions OOTB
<li> 3. Add table exception rules, allow users to decide which table to auto optimize and which to manually override if they want to optimize their own
<li> 4. Dynamically figure out job configuration (cluster size / periodicity) of commands to run
  
#### Query Statistics: 

<li> 1. Enable parsing of queries from not just DBSQL, but ALL clusters (jobs/AP)
<li> 2. Enable parameter selection for specifying specific (1 or many) databases to scrape
<li> 3. Enable pointing to a Git location to parse SQL files with SELECT statements in GIT

#### Transaction Log Statistics: 

<li> 1. Add partition filtering and file size management - <b> DONE </b>
<li> 2. Column Reording first 32 - <b> IN PROGRESS </b>  
<li> 3. Add Analyze Table STATS - <b> DONE </b>  

#### Ranking Statistics Algorithm:

<li> 1. More robust standard scaling for statistics (right now its 0-1 standard scaling partitioned by TABLE)
<li> 2. Standard scale Cardinality metric to weight rank in scaling (higher cardinality should get weighted more even if slightly lower on runtime/occurence)
<li> 3. Make ranking system more intelligent - open ended feedback needed for ideas on making ranking system more generalizable and nuanced
<li> 4. Dynamically prune for the actual number of ZORDER columns to best used (dependant first on cardinality). Do this possibly by tracking distance between certain statistics (i.e. if ColA appears 3000 times and Col B appears 2900 times, use both, but if ColA appears 3000 times but ColB appears 3 times, only use ColA)

</li>


#### Execution Step

<li> 1. Automatically create and schedule a job via the API that reads from the config with the provided notebook and runs at a parameter interval selected by the user
  
<li> 2. Use DLT to Generate DDL, file Sizes, and Managed these optimize statements automatically without actually needing to do ETL in DLT