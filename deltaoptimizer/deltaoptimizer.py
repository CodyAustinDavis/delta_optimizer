import json
import sqlparse
from sql_metadata import Parser
import requests
import re
import os
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession


class DeltaOptimizerBase():
    
    def __init__(self, database_name="delta_optimizer"):

        ## Assumes running on a spark environment

        self.database_name = database_name
        self.spark = SparkSession.getActiveSession()

        print(f"Initializing Delta Optimizer at database location: {self.database_name}")
        ### Initialize Tables on instantiation

        ## Create Database
        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.database_name};""")
        
        ## Query Profiler Tables
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_history_log 
                           (Id BIGINT GENERATED ALWAYS AS IDENTITY,
                           WarehouseIds ARRAY<STRING>,
                           WorkspaceName STRING,
                           StartTimestamp TIMESTAMP,
                           EndTimestamp TIMESTAMP)
                           USING DELTA""")

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.raw_query_history_statistics
                        (Id BIGINT GENERATED ALWAYS AS IDENTITY,
                        query_id STRING,
                        query_start_time_ms FLOAT,
                        query_end_time_ms FLOAT,
                        duration FLOAT,
                        query_text STRING,
                        status STRING,
                        statement_type STRING,
                        rows_produced FLOAT,
                        metrics MAP<STRING, FLOAT>)
                        USING DELTA""")

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.parsed_distinct_queries
                        (
                        Id BIGINT GENERATED ALWAYS AS IDENTITY,
                        query_id STRING,
                        query_text STRING,
                        profiled_columns ARRAY<STRING>
                        )
                        USING DELTA""")
        
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.read_statistics_scaled_results
                (
                TableName STRING,
                ColumnName STRING,
                isUsedInJoin INTEGER,
                isUsedInFilter INTEGER,
                isUsedInGroup INTEGER,
                NumberOfQueriesUsedInJoin LONG,
                NumberOfQueriesUsedInFilter LONG,
                NumberOfQueriesUsedInGroup LONG,
                QueryRefereneCount LONG,
                RawTotalRuntime DOUBLE,
                AvgQueryDuration DOUBLE,
                TotalColumnOccurrencesForAllQueries LONG,
                AvgColumnOccurrencesInQueryies DOUBLE,
                QueryReferenceCountScaled DOUBLE,
                RawTotalRuntimeScaled DOUBLE,
                AvgQueryDurationScaled DOUBLE,
                TotalColumnOccurrencesForAllQueriesScaled DOUBLE,
                AvgColumnOccurrencesInQueriesScaled DOUBLE
                )""")

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.read_statistics_column_level_summary
                (
                TableName STRING,
                ColumnName STRING,
                isUsedInJoin INTEGER,
                isUsedInFilter INTEGER,
                isUsedInGroup INTEGER,
                NumberOfQueriesUsedInJoin LONG,
                NumberOfQueriesUsedInFilter LONG,
                NumberOfQueriesUsedInGroup LONG,
                QueryRefereneCount LONG,
                RawTotalRuntime DOUBLE,
                AvgQueryDuration DOUBLE,
                TotalColumnOccurrencesForAllQueries LONG,
                AvgColumnOccurrencesInQueryies DOUBLE
                )""")

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_summary_statistics (
                query_id STRING,
                AverageQueryDuration DOUBLE,
                AverageRowsProduced DOUBLE,
                TotalQueryRuns LONG,
                DurationTimesRuns DOUBLE
                )""")

        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.query_column_statistics
                (
                TableName STRING,
                ColumnName STRING,
                query_text STRING,
                query_id STRING,
                AverageQueryDuration DOUBLE,
                AverageRowsProduced DOUBLE,
                TotalQueryRuns LONG,
                DurationTimesRuns DOUBLE,
                NumberOfColumnOccurrences INTEGER,
                isUsedInJoin INTEGER,
                isUsedInFilter INTEGER,
                isUsedInGroup INTEGER
                )""")


        ## Transaction Log Analysis Tables
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.write_statistics_merge_predicate
            (
            TableName STRING,
            ColumnName STRING,
            HasColumnInMergePredicate INTEGER,
            NumberOfVersionsPredicateIsUsed INTEGER,
            AvgMergeRuntimeMs INTEGER,
            UpdateTimestamp TIMESTAMP)
            USING DELTA;
            """)
        
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.all_tables_cardinality_stats
             (TableName STRING,
             ColumnName STRING,
             SampleSize INTEGER,
             TotalCountInSample INTEGER,
             DistinctCountOfColumnInSample INTEGER,
             CardinalityProportion FLOAT,
             CardinalityProportionScaled FLOAT,
             IsUsedInReads INTEGER,
             IsUsedInWrites INTEGER,
             UpdateTimestamp TIMESTAMP)
             USING DELTA""")



        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.all_tables_table_stats
                     (TableName STRING,
                     sizeInBytes FLOAT,
                     sizeInGB FLOAT,
                     partitionColumns ARRAY<STRING>,
                     mappedFileSizeInMb STRING,
                     UpdateTimestamp TIMESTAMP)
                     USING DELTA""")
        
        ## Optimization Strategy Tables
        
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.database_name}.final_ranked_cols_by_table
            (
            TableName STRING,
            ColumnName STRING ,
            SampleSize INTEGER,
            TotalCountInSample INTEGER,
            DistinctCountOfColumnInSample INTEGER,
            CardinalityProportion FLOAT,
            CardinalityProportionScaled FLOAT,
            IsUsedInReads INTEGER,
            IsUsedInWrites INTEGER,
            UpdateTimestamp TIMESTAMP,
            IsPartitionCol INTEGER,
            QueryReferenceCountScaled DOUBLE,
            RawTotalRuntimeScaled DOUBLE,
            AvgQueryDurationScaled DOUBLE,
            TotalColumnOccurrencesForAllQueriesScaled DOUBLE,
            AvgColumnOccurrencesInQueriesScaled DOUBLE,
            isUsedInJoin INTEGER,
            isUsedInFilter INTEGER,
            isUsedInGroup INTEGER,
            RawScore DOUBLE,
            ColumnRank INTEGER,
            RankUpdateTimestamp TIMESTAMP
            )
            USING DELTA
        """)


        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS  {self.database_name}.final_optimize_config
                (TableName STRING NOT NULL,
                ZorderCols ARRAY<STRING>,
                OptimizeCommandString STRING, --OPTIMIZE OPTIONAL < ZORDER BY >
                AlterTableCommandString STRING, --delta.targetFileSize, delta.tuneFileSizesForRewrites
                AnalyzeTableCommandString STRING -- ANALYZE TABLE COMPUTE STATISTICS
                )
                USING DELTA
        """)
        
        return
    
        ## Clear database and start over
    def truncate_delta_optimizer_results(self):
        
        print(f"Truncating database (and all tables within): {self.database_name}...")
        df = self.spark.sql(f"""SHOW TABLES IN {self.database_name}""").filter(F.col("database") == F.lit(self.database_name)).select("tableName").collect()

        for i in df: 
            table_name = i[0]
            print(f"Deleting Table: {table_name}...")
            self.spark.sql(f"""TRUNCATE TABLE {self.database_name}.{table_name}""")
            
        print(f"Database: {self.database_name} successfully Truncated!")
        return
    
    ## Add functions to truncate or remove tables
    
        ## Add functions to truncate or remove tables
    def vacuum_optimizer_tables(self):
        
        print(f"vacuuming tables database (and all tables within): {self.database_name}...")
        df = self.spark.sql(f"""SHOW TABLES IN {self.database_name}""").filter(F.col("database") == F.lit(self.database_name)).select("tableName").collect()

        for i in df: 
            table_name = i[0]
            print(f"Vacuuming Table: {table_name}...")
            self.spark.sql(f"""VACUUM {self.database_name}.{table_name};""")
            
        print(f"Database: {self.database_name} successfully cleaned up!")
        return  

    
##################################

######## Query Profiler ##########

class QueryProfiler(DeltaOptimizerBase):
    
    def __init__(self, workspace_url, warehouse_ids, database_name="delta_optimizer"):
        
        ## Assumes running on a spark environment
        super().__init__(database_name=database_name)
        
        self.workspace_url = workspace_url.strip()
        self.warehouse_ids_list = [i.strip() for i in warehouse_ids]
        self.warehouse_ids = ",".join(self.warehouse_ids_list)
        
        print(f"Initializing Delta Optimizer for: {self.workspace_url}\n Monitoring SQL Warehouses: {self.warehouse_ids} \n Database Location: {self.database_name}")
        ### Initialize Tables on instantiation
  
        return
    
    
    ## Spark SQL Tree Parsing Udfs    
    ## Input Filter Type can be : where, join, group_by
    @staticmethod
    @F.udf("array<string>")
    def getParsedFilteredColumnsinSQL(sqlString):

        ## output ["table_name:column_name,table_name:colunmn:name"]
        final_table_map = []

        try: 
            results = Parser(sqlString)

            final_columns = []

            ## If just a select, then skip this and just return the table
            try:
                final_columns.append(results.columns_dict.get("where"))
                final_columns.append(results.columns_dict.get("join"))
                final_columns.append(results.columns_dict.get("group_by"))

            except:
                for tbl in results.tables:
                    final_table_map.append(f"{tbl}:")

            final_columns_clean = [i for i in final_columns if i is not None]
            flatted_final_cols = list(set([x for xs in final_columns_clean for x in xs]))

            ## Try to map columns to specific tables for simplicity downstream

            """Rules -- this isnt a strict process cause we will filter things later, 
            what needs to happen is we need to get all possible columns on a given table, even if not true

            ## Aliases are already parsed so the column is either fully qualified or fully ambiguous
            ## Assign a column to table if: 
            ## 1. column has an explicit alias to it
            ## 2. column is not aliased
            """

            for tbl in results.tables:
                found_cols = []
                for st in flatted_final_cols:

                    ## Get Column Part
                    try:
                        column_val = st[st.rindex('.')+1:] 
                    except: 
                        column_val = st

                    ## Get Table Part
                    try:
                        table_val = st[:st.rindex('.')] or None
                    except:
                        table_val = None

                    ## Logic that add column if tbl name is found or if there was no specific table name for the column
                    if st.find(tbl) >= 0 or (table_val is None):
                        if column_val is not None and len(column_val) > 1:
                            final_table_map.append(f"{tbl}:{column_val}")

        except Exception as e:
            final_table_map = [str(f"ERROR: {str(e)}")]

        return final_table_map

    @staticmethod
    @F.udf("integer")
    def checkIfJoinColumn(sqlString, columnName):
        try: 
            results = Parser(sqlString)

            ## If just a select, then skip this and just return the table
            if columnName in results.columns_dict.get("join"):
                return 1
            else:
                return 0
        except:
            return 0


    @staticmethod
    @F.udf("integer")
    def checkIfFilterColumn(sqlString, columnName):
        try: 
            results = Parser(sqlString)

            ## If just a select, then skip this and just return the table
            if columnName in results.columns_dict.get("where"):
                return 1
            else:
                return 0
        except:
            return 0

    @staticmethod
    @F.udf("integer")
    def checkIfGroupColumn(sqlString, columnName):
        try: 
            results = Parser(sqlString)

            ## If just a select, then skip this and just return the table
            if columnName in results.columns_dict.get("group_by"):
                return 1
            else:
                return 0
        except:
            return 0

    ## Convert timestamp to milliseconds for API
    @staticmethod
    def ms_timestamp(dt):
        return int(round(dt.replace(tzinfo=timezone.utc).timestamp() * 1000, 0))
    
    
    ## Get Start and End range for Query History API
    def get_time_series_lookback(self, lookback_period):
        
        ## Gets time series from current timestamp to now - lookback period - if overrride
        end_timestamp = datetime.now()
        start_timestamp = end_timestamp - timedelta(days = lookback_period)
        ## Convert to ms
        start_ts_ms = self.ms_timestamp(start_timestamp)
        end_ts_ms = self.ms_timestamp(end_timestamp)
        print(f"Getting Query History to parse from period: {start_timestamp} to {end_timestamp}")
        return start_ts_ms, end_ts_ms
 

    ## If no direct time ranges provided (like after a first load, just continue where the log left off)
    def get_most_recent_history_from_log(self, mode='auto', lookback_period=3):
      
        ## This function gets the most recent end timestamp of the query history range, and returns new range from then to current timestamp
        
        start_timestamp = self.spark.sql(f"""SELECT MAX(EndTimestamp) FROM {self.database_name}.query_history_log""").collect()[0][0]
        end_timestamp = datetime.now()
        
        if (start_timestamp is None or mode != 'auto'): 
            if mode == 'auto' and start_timestamp is None:
                print(f"""Mode is auto and there are no previous runs in the log... using lookback period from today: {lookback_period}""")
            elif mode != 'auto' and start_timestamp is None:
                print(f"Manual time interval with lookback period: {lookback_period}")
                
            return self.get_time_series_lookback(lookback_period)
        
        else:
            start_ts_ms = self.ms_timestamp(start_timestamp)
            end_ts_ms = self.ms_timestamp(end_timestamp)
            print(f"Getting Query History to parse from most recent pull at: {start_timestamp} to {end_timestamp}")
            return start_ts_ms, end_ts_ms
    
    
    ## Insert a query history pull into delta log to track state
    def insert_query_history_delta_log(self, start_ts_ms, end_ts_ms):

        ## Upon success of a query history pull, this function logs the start_ts and end_ts that it was pulled into the logs

        try: 
            spark.sql(f"""INSERT INTO {self.database_name}.query_history_log (WarehouseIds, WorkspaceName, StartTimestamp, EndTimestamp)
                               VALUES(split('{self.warehouse_ids}', ','), 
                               '{self.workspace_url}', ('{start_ts_ms}'::double/1000)::timestamp, 
                               ('{end_ts_ms}'::double/1000)::timestamp)
            """)
        except Exception as e:
            raise(e)
    
    

    ## Run the Query History Pull (main function)
    def build_query_history_profile(self, dbx_token, mode='auto', lookback_period_days=3):
        
        ## Modes are 'auto' and 'manual' - auto, means it manages its own state, manual means you override the time frame to analyze no matter the history
        lookback_period = int(lookback_period_days)
        warehouse_ids_list = self.warehouse_ids_list
        workspace_url = self.workspace_url

        print(f"""Loading Query Profile to delta from workspace: {workspace_url} \n 
              from Warehouse Ids: {warehouse_ids_list} \n for the last {lookback_period} days...""")
        
        ## Get time series range based on 
        ## If override = True, then choose lookback period in days
        start_ts_ms, end_ts_ms = self.get_most_recent_history_from_log(mode, lookback_period)
        
        ## Put together request 
        
        request_string = {
            "filter_by": {
              "query_start_time_range": {
              "end_time_ms": end_ts_ms,
              "start_time_ms": start_ts_ms
            },
            "statuses": [
                "FINISHED", "CANCELED"
            ],
            "warehouse_ids": warehouse_ids_list
            },
            "include_metrics": "true",
            "max_results": "1000"
        }

        ## Convert dict to json
        v = json.dumps(request_string)
        
        uri = f"https://{workspace_url}/api/2.0/sql/history/queries"
        headers_auth = {"Authorization":f"Bearer {dbx_token}"}

        
        ## This file could be large
        ## Convert response to dict
        
        #### Get Query History Results from API
        endp_resp = requests.get(uri, data=v, headers=headers_auth).json()
        
        initial_resp = endp_resp.get("res")
        
        if initial_resp is None:
            print(f"DBSQL Has no queries on the warehouse for these times:{start_ts_ms} - {end_ts_ms}")
            initial_resp = []
            ## Continue anyways cause there could be old queries and we want to still compute aggregates
        
        
        next_page = endp_resp.get("next_page_token")
        has_next_page = endp_resp.get("has_next_page")
        

        if has_next_page is True:
            print(f"Has next page?: {has_next_page}")
            print(f"Getting next page: {next_page}")

        ## Page through results   
        page_responses = []

        while has_next_page is True: 

            print(f"Getting results for next page... {next_page}")

            raw_page_request = {
            "include_metrics": "true",
            "max_results": 1000,
            "page_token": next_page
            }

            json_page_request = json.dumps(raw_page_request)

            ## This file could be large
            current_page_resp = requests.get(uri,data=json_page_request, headers=headers_auth).json()
            current_page_queries = current_page_resp.get("res")

            ## Add Current results to total results or write somewhere (to s3?)

            page_responses.append(current_page_queries)

            ## Get next page
            next_page = current_page_resp.get("next_page_token")
            has_next_page = current_page_resp.get("has_next_page")

            if has_next_page is False:
                break

                
        ## Coaesce all responses     
        all_responses = [x for xs in page_responses for x in xs] + initial_resp
        print(f"Saving {len(all_responses)} Queries To Delta for Profiling")

        
        ## Start Profiling Process
        try: 
            
            ## Get responses and save to Delta 
            raw_queries_df = (self.spark.createDataFrame(all_responses))
            raw_queries_df.createOrReplaceTempView("raw")

            self.spark.sql(f"""INSERT INTO {self.database_name}.raw_query_history_statistics (query_id,query_start_time_ms, query_end_time_ms, duration, query_text, status, statement_type,rows_produced,metrics)
                        SELECT
                        query_id,
                        query_start_time_ms,
                        query_end_time_ms,
                        duration,
                        query_text,
                        status,
                        statement_type,
                        rows_produced,
                        metrics
                        FROM raw
                        WHERE statement_type = 'SELECT';
                        """)
            
            ## If successfull, insert log
            self.insert_query_history_delta_log(start_ts_ms, end_ts_ms)
            
            
            ## Build Aggregate Summary Statistics with old and new queries
            self.spark.sql(f"""
                --Calculate Query Statistics to get importance Rank by Query (duration, rows_returned)
                -- This is an AGGREGATE table that needs to be rebuilt every time from the source -- not incremental
                CREATE OR REPLACE TABLE {self.database_name}.query_summary_statistics
                AS (
                  WITH raw_query_stats AS (
                    SELECT query_id,
                    AVG(duration) AS AverageQueryDuration,
                    AVG(rows_produced) AS AverageRowsProduced,
                    COUNT(*) AS TotalQueryRuns,
                    AVG(duration)*COUNT(*) AS DurationTimesRuns
                    FROM {self.database_name}.raw_query_history_statistics
                    WHERE status IN('FINISHED', 'CANCELED')
                    AND statement_type = 'SELECT'
                    GROUP BY query_id
                  )
                  SELECT 
                  *
                  FROM raw_query_stats
                )
                """)
            
            ## Parse SQL Query and Save into parsed distinct queries table
            df = self.spark.sql(f"""SELECT DISTINCT query_id, query_text FROM {self.database_name}.raw_query_history_statistics""")

            df_profiled = (df.withColumn("profiled_columns", self.getParsedFilteredColumnsinSQL(F.col("query_text")))
                    )

            df_profiled.createOrReplaceTempView("new_parsed")
            self.spark.sql(f"""
                MERGE INTO {self.database_name}.parsed_distinct_queries AS target
                USING new_parsed AS source
                ON source.query_id = target.query_id
                WHEN MATCHED THEN UPDATE SET target.query_text = source.query_text
                WHEN NOT MATCHED THEN 
                    INSERT (target.query_id, target.query_text, target.profiled_columns) 
                    VALUES (source.query_id, source.query_text, source.profiled_columns)""")
            
            ## Calculate statistics on profiled queries

            pre_stats_df = (self.spark.sql(f"""
                  WITH exploded_parsed_cols AS (SELECT DISTINCT
                  explode(profiled_columns) AS explodedCols,
                  query_id,
                  query_text
                  FROM {self.database_name}.parsed_distinct_queries
                  ),

                  step_2 AS (SELECT DISTINCT
                  split(explodedCols, ":")[0] AS TableName,
                  split(explodedCols, ":")[1] AS ColumnName,
                  root.query_text,
                  hist.*
                  FROM exploded_parsed_cols AS root
                  LEFT JOIN {self.database_name}.query_summary_statistics AS hist USING (query_id)--SELECT statements only included
                  )

                  SELECT *,
                  size(split(query_text, ColumnName)) - 1 AS NumberOfColumnOccurrences
                  FROM step_2
                """)
                .withColumn("isUsedInJoin", self.checkIfJoinColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
                .withColumn("isUsedInFilter", self.checkIfFilterColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
                .withColumn("isUsedInGroup", self.checkIfGroupColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
                )

            pre_stats_df.createOrReplaceTempView("withUseFlags")

            self.spark.sql(f"""
            CREATE OR REPLACE TABLE {self.database_name}.query_column_statistics
            AS SELECT * FROM withUseFlags
            """)

            #### Calculate more statistics

            self.spark.sql(f"""CREATE OR REPLACE TABLE {self.database_name}.read_statistics_column_level_summary
                    AS
                    WITH test_q AS (
                        SELECT * FROM {self.database_name}.query_column_statistics
                        WHERE length(ColumnName) >= 1 -- filter out queries with no joins or predicates TO DO: Add database filtering here

                    ),
                    step_2 AS (
                        SELECT 
                        TableName,
                        ColumnName,
                        MAX(isUsedInJoin) AS isUsedInJoin,
                        MAX(isUsedInFilter) AS isUsedInFilter,
                        MAX(isUsedInGroup) AS isUsedInGroup,
                        SUM(isUsedInJoin) AS NumberOfQueriesUsedInJoin,
                        SUM(isUsedInFilter) AS NumberOfQueriesUsedInFilter,
                        SUM(isUsedInGroup) AS NumberOfQueriesUsedInGroup,
                        COUNT(DISTINCT query_id) AS QueryReferenceCount,
                        SUM(DurationTimesRuns) AS RawTotalRuntime,
                        AVG(AverageQueryDuration) AS AvgQueryDuration,
                        SUM(NumberOfColumnOccurrences) AS TotalColumnOccurrencesForAllQueries,
                        AVG(NumberOfColumnOccurrences) AS AvgColumnOccurrencesInQueryies
                        FROM test_q
                        WHERE length(ColumnName) >=1
                        GROUP BY TableName, ColumnName
                    )
                    SELECT * FROM step_2
                    ; """)


            #### Standard scale the metrics 
            df = self.spark.sql(f"""SELECT * FROM {self.database_name}.read_statistics_column_level_summary""")

            columns_to_scale = ["QueryReferenceCount", 
                                "RawTotalRuntime", 
                                "AvgQueryDuration", 
                                "TotalColumnOccurrencesForAllQueries", 
                                "AvgColumnOccurrencesInQueryies"]

            min_exprs = {x: "min" for x in columns_to_scale}
            max_exprs = {x: "max" for x in columns_to_scale}

            ## Apply basic min max scaling by table for now

            dfmin = df.groupBy("TableName").agg(min_exprs)
            dfmax = df.groupBy("TableName").agg(max_exprs)

            df_boundaries = dfmin.join(dfmax, on="TableName", how="inner")

            df_pre_scaled = df.join(df_boundaries, on="TableName", how="inner")

            df_scaled = (df_pre_scaled
                     .withColumn("QueryReferenceCountScaled", F.coalesce((F.col("QueryReferenceCount") - F.col("min(QueryReferenceCount)"))/(F.col("max(QueryReferenceCount)") - F.col("min(QueryReferenceCount)")), F.lit(0)))
                     .withColumn("RawTotalRuntimeScaled", F.coalesce((F.col("RawTotalRuntime") - F.col("min(RawTotalRuntime)"))/(F.col("max(RawTotalRuntime)") - F.col("min(RawTotalRuntime)")), F.lit(0)))
                     .withColumn("AvgQueryDurationScaled", F.coalesce((F.col("AvgQueryDuration") - F.col("min(AvgQueryDuration)"))/(F.col("max(AvgQueryDuration)") - F.col("min(AvgQueryDuration)")), F.lit(0)))
                     .withColumn("TotalColumnOccurrencesForAllQueriesScaled", F.coalesce((F.col("TotalColumnOccurrencesForAllQueries") - F.col("min(TotalColumnOccurrencesForAllQueries)"))/(F.col("max(TotalColumnOccurrencesForAllQueries)") - F.col("min(TotalColumnOccurrencesForAllQueries)")), F.lit(0)))
                     .withColumn("AvgColumnOccurrencesInQueriesScaled", F.coalesce((F.col("AvgColumnOccurrencesInQueryies") - F.col("min(AvgColumnOccurrencesInQueryies)"))/(F.col("max(AvgColumnOccurrencesInQueryies)") - F.col("min(AvgColumnOccurrencesInQueryies)")), F.lit(0)))
                     .selectExpr("TableName", "ColumnName", "isUsedInJoin", "isUsedInFilter","isUsedInGroup","NumberOfQueriesUsedInJoin","NumberOfQueriesUsedInFilter","NumberOfQueriesUsedInGroup","QueryReferenceCount", "RawTotalRuntime", "AvgQueryDuration", "TotalColumnOccurrencesForAllQueries", "AvgColumnOccurrencesInQueryies", "QueryReferenceCountScaled", "RawTotalRuntimeScaled", "AvgQueryDurationScaled", "TotalColumnOccurrencesForAllQueriesScaled", "AvgColumnOccurrencesInQueriesScaled")
                        )


            df_scaled.createOrReplaceTempView("final_scaled_reads")

            self.spark.sql(f"""CREATE OR REPLACE TABLE {self.database_name}.read_statistics_scaled_results 
            AS
            SELECT * FROM final_scaled_reads""")


            print(f"""Completed Query Profiling! Results can be found here:\n
            SELECT * FROM {self.database_name}.read_statistics_scaled_results""")

            return
            
        except Exception as e:
            print(f"Could not profile query history of ({len(all_responses)}) queries. :( There may not be any queries to profile since last run, either ignore or switch to manual mode with a further lookback window...")
            raise(e)


##################################
  
######## Delta Profiler ##########
class DeltaProfiler(DeltaOptimizerBase):
    
    def __init__(self, monitored_db_csv = 'all', database_name="delta_optimizer"):
        
        super().__init__(database_name=database_name)
        self.db_list = [x.strip() for x in monitored_db_csv.split(",") if x != '']
        self.table_list = []
    
        if (len(self.db_list) == 1 and self.db_list[0].lower() == 'all') or len(self.db_list) == 0:
            self.mode = 'all'
        else:
            self.mode = 'subset'
            
        ## File size map
        self.file_size_map = [{"max_table_size_gb": 8, "file_size": '16mb'},
                 {"max_table_size_gb": 16, "file_size": '32mb'},
                 {"max_table_size_gb": 32, "file_size": '64mb'},
                 {"max_table_size_gb": 64, "file_size": '128mb'},
                 {"max_table_size_gb": 128, "file_size": '128mb'},
                 {"max_table_size_gb": 256, "file_size": '128mb'},
                 {"max_table_size_gb": 512, "file_size": '256mb'},
                 {"max_table_size_gb": 1024, "file_size": '307mb'},
                 {"max_table_size_gb": 2560, "file_size": '512mb'},
                 {"max_table_size_gb": 3072, "file_size": '716mb'},
                 {"max_table_size_gb": 5120, "file_size": '1gb'},
                 {"max_table_size_gb": 7168, "file_size": '1gb'},
                 {"max_table_size_gb": 10240, "file_size": '1gb'},
                 {"max_table_size_gb": 51200, "file_size": '1gb'},
                 {"max_table_size_gb": 102400, "file_size": '1gb'}]


        self.file_size_df = (self.spark.createDataFrame(self.file_size_map))
        
        ## df of tables initilization
        self.tbl_df = self.spark.sql("show tables in default like 'xxx_delta_optimizer'")

        return
    
    ### Static methods / udfs
    @staticmethod
    @F.udf("string")
    def buildCardinalitySampleSQLStatement(table_name, column_list, sample_size:float):

        ## This query ensures that it does not scan the whole table and THEN limits
        sample_string = f"WITH sample AS (SELECT * FROM {table_name} LIMIT {sample_size})"
        sql_from = f" FROM sample"
        str2 = [" SELECT COUNT(0) AS TotalCount"]

        for i in column_list:
            sql_string = f"COUNT(DISTINCT {i}) AS DistinctCountOf_{i}"
            str2.append(sql_string)


        final_sql = sample_string + ", ".join(str2) + sql_from

        return final_sql
    
    
    def get_table_list(self):
        return set(self.table_list)
    
    
    def get_all_tables_to_monitor(self):
        
        try:
            #Loop through all databases
            for db in self.spark.sql("""show databases""").filter(F.col("databaseName").isin(self.db_list)).collect():
              #create a dataframe with list of tables from the database
              df = self.spark.sql(f"show tables in {db.databaseName}")
              #union the tables list dataframe with main dataframe 
              self.tbl_df = self.tbl_df.union(df)

            ## Exclude temp views/tables
            self.tbl_df = (self.tbl_df.filter(F.col("isTemporary") == F.lit('false')))

            ## Check if in selected databases if mode is not all

            if self.mode == "subset":

                self.tbl_df = self.tbl_df.filter(F.col("database").isin(self.db_list))

            self.tbl_df.createOrReplaceTempView("all_tables")

            df_tables = self.spark.sql("""
            SELECT 
            concat(database, '.', tableName) AS fully_qualified_table_name
            FROM all_tables
            """).collect()

            self.table_list = [i[0] for i in df_tables]

            #print(f"Tables Gather for Merge Predicate Analysis: \n {self.table_list}")
            
        except Exception as e:
            raise(e)
        
        return

    
    ## Parse Transaction Log
    def parse_stats_for_tables(self):
        
        ## If you only run this, must get tables to monitor first
        self.get_all_tables_to_monitor()
        
        for tbl in set(self.table_list):
            print(f"Running History Analysis for Table: {tbl}")

            try: 

                ## Get Transaction log with relevant transactions
                hist_df = self.spark.sql(f"""
                WITH hist AS
                (DESCRIBE HISTORY {tbl}
                )

                SELECT version, timestamp,
                operationParameters.predicate,
                operationMetrics.executionTimeMs
                FROM hist
                WHERE operation IN ('MERGE', 'DELETE')
                ;
                """)

                hist_df.createOrReplaceTempView("hist_df")

                ## Get DF of Columns for that table

                df_cols = [Row(i) for i in self.spark.sql(f"""SELECT * FROM {tbl}""").columns]

                df = self.spark.sparkContext.parallelize(df_cols).toDF().withColumnRenamed("_1", "Columns")

                df.createOrReplaceTempView("df_cols")

                ## Calculate stats for this table

                df_stats = (self.spark.sql("""
                -- Full Cartesian product small table.. maybe since one table at a time... parallelize later 
                WITH raw_results AS (
                SELECT 
                *,
                predicate LIKE (concat('%',`Columns`::string,'%')) AS HasColumnInMergePredicate
                FROM df_cols
                JOIN hist_df
                )

                SELECT Columns AS ColumnName,
                CASE WHEN MAX(HasColumnInMergePredicate) = 'true' THEN 1 ELSE 0 END AS HasColumnInMergePredicate,
                COUNT(DISTINCT CASE WHEN HasColumnInMergePredicate = 'true' THEN `version` ELSE NULL END)::integer AS NumberOfVersionsPredicateIsUsed,
                AVG(executionTimeMs::integer) AS AvgMergeRuntimeMs
                FROM raw_results
                GROUP BY Columns
                """)
                  .withColumn("TableName", F.lit(tbl))
                  .withColumn("UpdateTimestamp", F.current_timestamp())
                  .select("TableName", "ColumnName", "HasColumnInMergePredicate", "NumberOfVersionsPredicateIsUsed", "AvgMergeRuntimeMs", "UpdateTimestamp")
                 )

                (df_stats.createOrReplaceTempView("source_stats"))

                self.spark.sql(f"""MERGE INTO {self.database_name}.write_statistics_merge_predicate AS target
                USING source_stats AS source
                ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
                WHEN MATCHED THEN UPDATE SET * 
                WHEN NOT MATCHED THEN INSERT *
                """)

            except Exception as e:
                print(f"Skipping analysis for table {tbl} for error: {str(e)}")
                pass
            
        return
    
    
    
    ## Create final transaction log stats tables
    def build_all_tables_stats(self):
        
        ## If you only run this, must get tables to monitor first
        self.get_all_tables_to_monitor()
        
        for tbl in set(self.table_list):

            print(f"Prepping Delta Table Stats: {tbl}")

            try: 
                df_cols = [Row(i) for i in self.spark.sql(f"""SELECT * FROM {tbl}""").columns]

                df = self.spark.sparkContext.parallelize(df_cols).toDF().withColumnRenamed("_1", "ColumnName").withColumn("TableName", F.lit(tbl))

                df.createOrReplaceTempView("df_cols")

                ## This needs to be re-worked
                self.spark.sql(f"""INSERT INTO {self.database_name}.all_tables_cardinality_stats
                SELECT TableName, ColumnName, NULL, NULL, NULL, NULL, NULL, NULL, NULL, current_timestamp() FROM df_cols AS source
                WHERE NOT EXISTS (SELECT 1 FROM {self.database_name}.all_tables_cardinality_stats ss 
                                  WHERE ss.TableName = source.TableName AND ss.ColumnName = source.ColumnName)
                """)

            except Exception as e:
                print(f"Skipping analysis for table {tbl} for error: {str(e)}")
                pass

            print(f"Collecting Size and Partition Stats for : {tbl}")


            try: 
                table_df = (self.spark.sql(f"""DESCRIBE DETAIL {tbl}""")
                    .selectExpr("name", "sizeInBytes", "sizeInBytes/(1024*1024*1024) AS sizeInGB", "partitionColumns")
                            )

                table_df.createOrReplaceTempView("table_core")
                self.file_size_df.createOrReplaceTempView("file_size_map")

                ## !!! Not idempotent, must choose must recent version to work off of
                self.spark.sql(f"""
                WITH ss AS (
                    SELECT 
                    spine.*,
                    file_size AS mapped_file_size,
                    ROW_NUMBER() OVER (PARTITION BY name ORDER BY max_table_size_gb) AS SizeRank
                    FROM table_core AS spine
                    LEFT JOIN file_size_map AS fs ON spine.sizeInGB::integer <= fs.max_table_size_gb::integer
                    )
                    -- Pick smaller file size config by table size
                    INSERT INTO {self.database_name}.all_tables_table_stats
                    SELECT
                    name::string AS TableName, 
                    sizeInBytes::float AS sizeInBytes,
                    sizeInGB::float AS sizeInGB,
                    partitionColumns::array<string> AS partitionColumns,
                    mapped_file_size::string AS mappedFileSize,
                    current_timestamp() AS UpdateTimestamp
                    FROM ss WHERE SizeRank = 1 
                  """)
                
            except Exception as e:

                print(f"Failed to parse stats for {tbl} with error: {str(e)}")
                continue
                
        ## Optimize tables (ironic? ahah)
        print("Optimizing final tables to prepare to strategy ranking...")
        self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_cardinality_stats""")
        self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_table_stats""")
        return
    
    
    def build_cardinality_stats(self, sample_size = 1000000):
        
        
        ## Check and see which tables/columns you even need to build statements for
        self.spark.sql(f"""WITH filter_cols AS (
            SELECT DISTINCT
            spine.TableName,
            spine.ColumnName,
            CASE WHEN reads.QueryReferenceCount >= 1 THEN 1 ELSE 0 END AS IsUsedInReads,
            CASE WHEN writes.HasColumnInMergePredicate >= 1 THEN 1 ELSE 0 END AS IsUsedInWrites
            FROM {self.database_name}.all_tables_cardinality_stats AS spine
            LEFT JOIN {self.database_name}.read_statistics_scaled_results reads ON spine.TableName = reads.TableName AND spine.ColumnName = reads.ColumnName
            LEFT JOIN {self.database_name}.write_statistics_merge_predicate writes ON spine.TableName = writes.TableName AND spine.ColumnName = writes.ColumnName
            )
        MERGE INTO {self.database_name}.all_tables_cardinality_stats AS target
        USING filter_cols AS source ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
        WHEN MATCHED THEN UPDATE SET
        target.IsUsedInReads = source.IsUsedInReads,
        target.IsUsedInWrites = source.IsUsedInWrites
        """)
        
        
        ## Build Cardinality Stats
        df_cardinality = (
                self.spark.sql(f"""
                    SELECT TableName, collect_list(ColumnName) AS ColumnList
                    FROM {self.database_name}.all_tables_cardinality_stats
                    WHERE (IsUsedInReads > 0 OR IsUsedInWrites > 0) --If columns is not used in any joins or predicates, lets not do cardinality stats
                    GROUP BY TableName
                """)
                .withColumn("cardinalityStatsStatement", self.buildCardinalitySampleSQLStatement(F.col("TableName"), F.col("ColumnList"), F.lit(sample_size)))
        )
        # Take sample size of 1M, if table is smaller, index on the count
        
        ## Build config to loop through inefficiently like a noob
        cardinality_statement = df_cardinality.collect()
        cardinality_config = {i[0]: {"columns": i[1], "sql": i[2]} for i in cardinality_statement}
        
        ## Loop through and build samples
        for i in cardinality_config:
            try:

                print(f"Building Cardinality Statistics for {i} ... \n")

                ## Gets cardinality stats on tables at a time, but all columns in parallel, then pivots results to long form
                wide_df = (self.spark.sql(cardinality_config.get(i).get("sql")))
                table_name = i
                clean_list = [ "'" + re.search('[^_]*_(.*)', i).group(1) + "'" + ", " + i for i in wide_df.columns if re.search('[^_]*_(.*)', i) is not None]
                clean_expr = ", ".join(clean_list)
                unpivot_Expr = f"stack({len(clean_list)}, {clean_expr}) as (ColumnName,ColumnDistinctCount)"	

                unpivot_DataFrame = (wide_df.select(F.expr(unpivot_Expr), "TotalCount").withColumn("TableName", F.lit(table_name))
                                     .withColumn("CardinalityProportion", F.col("ColumnDistinctCount").cast("double")/F.col("TotalCount").cast("double"))
                                    )

                ## Standard Mix/Max Scale Proportion
                columns_to_scale = ["CardinalityProportion"]
                min_exprs = {x: "min" for x in columns_to_scale}
                max_exprs = {x: "max" for x in columns_to_scale}

                ## Apply basic min max scaling by table for now

                dfmin = unpivot_DataFrame.groupBy("TableName").agg(min_exprs)
                dfmax = unpivot_DataFrame.groupBy("TableName").agg(max_exprs)

                df_boundaries = dfmin.join(dfmax, on="TableName", how="inner")

                df_pre_scaled = unpivot_DataFrame.join(df_boundaries, on="TableName", how="inner")

                df_scaled = (df_pre_scaled
                         .withColumn("CardinalityScaled", F.coalesce((F.col("CardinalityProportion") - F.col("min(CardinalityProportion)"))/(F.col("max(CardinalityProportion)") - F.col("min(CardinalityProportion)")), F.lit(0)))
                            )

                #display(df_scaled.orderBy("TableName"))

                df_scaled.createOrReplaceTempView("card_stats")

                self.spark.sql(f"""
                    MERGE INTO {self.database_name}.all_tables_cardinality_stats AS target 
                    USING card_stats AS source ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
                    WHEN MATCHED THEN UPDATE SET
                    target.SampleSize = CAST({sample_size} AS INTEGER),
                    target.TotalCountInSample = source.TotalCount,
                    target.DistinctCountOfColumnInSample = source.ColumnDistinctCount,
                    target.CardinalityProportion = (CAST(ColumnDistinctCount AS DOUBLE) / CAST(TotalCount AS DOUBLE)),
                    target.CardinalityProportionScaled = source.CardinalityScaled::double,
                    target.UpdateTimestamp = current_timestamp()
                """)

            except Exception as e:
                print(f"Skipping table {i} due to error {str(e)} \n")
                pass
        
        ## Optimize tables (ironic? ahah)
        self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_cardinality_stats""")
        self.spark.sql(f"""OPTIMIZE {self.database_name}.all_tables_table_stats""")
        
        return
        
#####################################################

######## Delta Optimizer Class ##########

class DeltaOptimizer(DeltaOptimizerBase):
    
    def __init__(self, database_name="delta_optimizer"):
        
        super().__init__(database_name=database_name)
        
        return
    
    @staticmethod
    @F.udf("string")
    def getAnalyzeTableCommand(inputTableName, tableSizeInGb, relativeColumns):

        ### Really basic heuristic to calculate statistics, can increase nuance in future versions
        tableSizeInGbLocal = float(tableSizeInGb)

        if tableSizeInGbLocal <= 100:
            sqlExpr = f"ANALYZE TABLE {inputTableName} COMPUTE STATISTICS FOR ALL COLUMNS;"
            return sqlExpr
        else:

            rel_Cols = str(relativeColumns).split(",")
            colExpr = ", ".join(rel_Cols)
            sqlExpr = f"ANALYZE TABLE {inputTableName} COMPUTE STATISTICS FOR COLUMNS {colExpr};"
            return sqlExpr

        
        
    @staticmethod
    @F.udf("string")   
    def getAlterTableCommand(inputTableName, fileSizeMapInMb, isMergeUsed):

        if float(isMergeUsed) >=1:

            alterExpr = f"ALTER TABLE {inputTableName} SET TBLPROPERTIES ('delta.targetFileSize' = '{fileSizeMapInMb}', 'delta.tuneFileSizesForRewrites' = 'true');"
            return alterExpr
        else: 
            alterExpr = f"ALTER TABLE {inputTableName} SET TBLPROPERTIES ('delta.targetFileSize' = '{fileSizeMapInMb}', 'delta.tuneFileSizesForRewrites' = 'false');"
            return alterExpr
        
        
    
    def build_optimization_strategy(self, optimize_method="both"):
        
        print("Building Optimization Plan for Monitored Tables...")
        
        try:
            self.spark.sql(f"""INSERT INTO {self.database_name}.final_ranked_cols_by_table
         
            WITH most_recent_table_stats AS (
            SELECT s1.*
            FROM {self.database_name}.all_tables_table_stats s1
            WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.all_tables_table_stats s2 WHERE s1.TableName = s2.TableName)
            ),

            final_stats AS (
            SELECT
            spine.*,
            CASE WHEN array_contains(tls.partitionColumns, spine.ColumnName) THEN 1 ELSE 0 END AS IsPartitionCol,
            QueryReferenceCountScaled,
            RawTotalRuntimeScaled,
            AvgQueryDurationScaled,
            TotalColumnOccurrencesForAllQueriesScaled,
            AvgColumnOccurrencesInQueriesScaled,
            isUsedInJoin,
            isUsedInFilter,
            isUsedInGroup
            FROM {self.database_name}.all_tables_cardinality_stats AS spine
            LEFT JOIN most_recent_table_stats AS tls ON tls.TableName = spine.TableName /* !! not idempotent, always choose most recent table stats !! */
            -- These are re-created every run
            LEFT JOIN {self.database_name}.read_statistics_scaled_results AS reads ON spine.TableName = reads.TableName AND spine.ColumnName = reads.ColumnName
            ),
            raw_scoring AS (
            -- THIS IS THE CORE SCORING EQUATION
            SELECT 
            *,
             CASE WHEN IsPartitionCol = 1 THEN 0 
             ELSE 
                 CASE 
                 WHEN '{optimize_method}' = 'Both'
                       THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) + (IsUsedInWrites*(0.001+COALESCE(CardinalityProportionScaled, 0)))
                 WHEN '{optimize_method}' = 'Read'
                       THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) /* If Read, do not add merge predicate to score */
                WHEN '{optimize_method}' = 'Write'
                        THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) + (5*IsUsedInWrites*(0.001+COALESCE(CardinalityProportionScaled, 0))) /* heavily weight the column such that it is always included */
                END
            END AS RawScore
            FROM final_stats
            ),
            -- Add cardinality in here somehow
            ranked_scores AS (
            SELECT 
            *,
            ROW_NUMBER() OVER( PARTITION BY TableName ORDER BY RawScore DESC) AS ColumnRank
            FROM raw_scoring
            )

            SELECT 
            *,
            current_timestamp() AS RankUpdateTimestamp /* not going to replace results each time, let optimizer choose most recent version and look at how it changes */
            FROM ranked_scores
            WHERE (ColumnRank <= 3::integer AND (IsUsedInReads + IsUsedInWrites) >= 1 AND DistinctCountOfColumnInSample >= 1000) /* 3 or less but must be high enough cardinality */
            OR (CardinalityProportion >= 0.2 AND RawScore IS NOT NULL) -- filter out max ZORDER cols, we will then collect list into OPTIMIZE string to run
            ORDER BY TableName, ColumnRank
    
            """)
            
            ##
            
            print(f"Completed Optimization Strategy Rankings! Results can be found here... \n SELECT * FROM {self.database_name}.final_ranked_cols_by_table")

            print(f"Now building final config file...")

            final_df = (self.spark.sql(f"""
                WITH final_results AS (
                SELECT s1.*
                FROM {self.database_name}.final_ranked_cols_by_table s1
                WHERE RankUpdateTimestamp = (SELECT MAX(RankUpdateTimestamp) FROM {self.database_name}.final_ranked_cols_by_table s2 
                                                                    WHERE s1.TableName = s2.TableName)
                ),
                  tt AS 
                          (
                              SELECT 
                              TableName, collect_list(ColumnName) AS ZorderCols
                              FROM final_results
                              GROUP BY TableName
                          )
                          SELECT 
                          *,
                          CASE WHEN size(ZorderCols) >=1 
                                  THEN concat("OPTIMIZE ", TableName, " ZORDER BY (", concat_ws(", ",ZorderCols), ");")
                              ELSE concat("OPTIMIZE ", TableName, ";")
                              END AS OptimizeCommandString,
                          NULL AS AlterTableCommandString,
                          NULL AS AnalyzeTableCommandString,
                          current_timestamp() AS UpdateTimestamp
                          FROM tt
                        """)
             )

            ### Save as single partition so collect is simple cause this should just be a config table
            ### This can have multiple recs per
            final_df.repartition(1).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{self.database_name}.final_optimize_config")

            #### Build Analyze Table String

            ## Build an ANALYZE TABLE command with the following heuristics: 

            ## 1. IF: table less than 100GB Run COMPUTE STATISTICS FOR ALL COLUMNS
            ## 2. ELSE IF: table great than 100GB, run COMPUTE STATISTICS FOR COLUMNS used in GROUP, FILTER, OR JOINS ONLY

            analyze_stats_df = (self.spark.sql(f"""
                WITH most_recent_table_stats AS (
                        SELECT s1.*
                        FROM {self.database_name}.all_tables_table_stats s1
                        WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.all_tables_table_stats s2 WHERE s1.TableName = s2.TableName)
                        ),
               stats_cols AS (
                    SELECT DISTINCT
                    spine.TableName,
                    card_stats.ColumnName
                    FROM most_recent_table_stats spine
                    LEFT JOIN {self.database_name}.all_tables_cardinality_stats AS card_stats ON card_stats.TableName = spine.TableName
                    LEFT JOIN {self.database_name}.read_statistics_scaled_results AS reads ON card_stats.TableName = reads.TableName 
                                                                                            AND reads.ColumnName = card_stats.ColumnName
                    WHERE card_stats.IsUsedInWrites = 1
                        OR (reads.isUsedInJoin + reads.isUsedInFilter + reads.isUsedInGroup ) >= 1
                    )
                    SELECT 
                    spine.TableName,
                    MAX(spine.sizeInGB) AS sizeInGB, -- We already chose most recnt file stats of each table
                    MAX(spine.mappedFileSizeInMb) AS fileSizeMap,
                    CASE WHEN MAX(IsUsedInWrites)::integer >= 1 THEN 1 ELSE 0 END AS ColumnsUsedInMerges, 
                    -- If table has ANY columns used in a merge predicate, tune file sizes for re-writes
                    concat_ws(',', array_distinct(collect_list(reads.ColumnName)))  AS ColumnsToCollectStatsOn
                    FROM most_recent_table_stats spine
                    LEFT JOIN {self.database_name}.all_tables_cardinality_stats AS card_stats ON card_stats.TableName = spine.TableName
                    LEFT JOIN stats_cols AS reads ON card_stats.TableName = reads.TableName AND reads.ColumnName = card_stats.ColumnName
                    GROUP BY spine.TableName
                    """)
                               )

            analyze_stats_completed = (analyze_stats_df
                                       .withColumn("AlterTableCommandString", self.getAlterTableCommand(F.col("TableName"), F.col("fileSizeMap"), F.col("ColumnsUsedInMerges")))
                                       .withColumn("AnalyzeTableCommandString", self.getAnalyzeTableCommand(F.col("TableName"), F.col("sizeInGB"), F.col("ColumnsToCollectStatsOn")))
                                      )

            analyze_stats_completed.createOrReplaceTempView("analyze_stats")

            self.spark.sql(f"""MERGE INTO {self.database_name}.final_optimize_config AS target
                            USING analyze_stats AS source
                            ON source.TableName = target.TableName
                            WHEN MATCHED THEN 
                            UPDATE SET 
                            target.AlterTableCommandString = source.AlterTableCommandString,
                            target.AnalyzeTableCommandString = source.AnalyzeTableCommandString
                      """)
        
            
            print(f"Table analysis complete!! Get results (dataFrame) by calling deltaOptimzer.get_results()!!")
            
            self.spark.sql(f"""CREATE OR REPLACE VIEW {self.database_name}.optimizer_results AS 
            WITH final_results AS (
            SELECT s1.*
            FROM {self.database_name}.final_optimize_config s1
            WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.final_optimize_config s2 WHERE s1.TableName = s2.TableName)
            )
            SELECT * FROM final_results;
            """)
            
            self.spark.sql(f"""CREATE OR REPLACE VIEW {self.database_name}.optimize_recent_rankings AS 
            WITH final_results AS (
            SELECT s1.*
            FROM {self.database_name}.final_ranked_cols_by_table s1
            WHERE RankUpdateTimestamp = (SELECT MAX(RankUpdateTimestamp) FROM {self.database_name}.final_ranked_cols_by_table s2 WHERE s1.TableName = s2.TableName)
            )
            SELECT * FROM final_results;
            """)
            
        except Exception as e:
            raise(e)
        
    
    def get_results(self):
        
        results_df =  (self.spark.sql(f""" 
         WITH final_results AS (
            SELECT s1.*
            FROM {self.database_name}.final_optimize_config s1
            WHERE UpdateTimestamp = (SELECT MAX(UpdateTimestamp) FROM {self.database_name}.final_optimize_config s2 WHERE s1.TableName = s2.TableName)
            )
        SELECT * FROM final_results;
        """))
        
        return results_df
    
    
    def get_rankings(self):
        
        rankings_df = (self.spark.sql(f"""
        WITH final_results AS (
        SELECT s1.*
        FROM {self.database_name}.final_ranked_cols_by_table s1
        WHERE RankUpdateTimestamp = (SELECT MAX(RankUpdateTimestamp) FROM {self.database_name}.final_ranked_cols_by_table s2 WHERE s1.TableName = s2.TableName)
        )
        SELECT * FROM final_results;
        """)
                   )

        return rankings_df

###############################################################


            
## Insert a query history pull into delta log to track state
if __name__ == '__main__':
    
    DBX_TOKEN = os.environ.get("DBX_TOKEN")
    
    ## Assume running in a Databricks notebook
    dbutils.widgets.dropdown("Query History Lookback Period (days)", defaultValue="3",choices=["1","3","7","14","30","60","90"])
    dbutils.widgets.text("SQL Warehouse Ids (csv list)", "")
    dbutils.widgets.text("Workspace DNS:", "")
    
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
    dbutils.widgets.text("Database Names (csv):", "")
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
    dbutils.widgets.dropdown("optimizeMethod", "Both", ["Reads", "Writes", "Both"])
    optimize_method = dbutils.widgets.get("optimizeMethod")
    
    ## Build Strategy
    delta_optimizer = DeltaOptimizer()

    delta_optimizer.build_optimization_strategy()
    