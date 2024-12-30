# ################################################## Module Information ################################################
#   Module Name         :   DQ Utility
#   Purpose             :   This module performs below operation:
#                               a. Read DQ checks to performed on a table 
#                               b. Perform DQ checks by creating the case-when statements
#                               
#                                
#   Input               :   dq checks
#   Output              :   dataframe with reords level info on dq status and summary
#   Pre-requisites      :   
#   Last changed on     :   22 Feb, 2024
#   Last changed by     :   Prakhar Chauhan
#   Reason for change   :   NA
# ######################################################################################################################
#Libraries
from pyspark.sql.functions import col,array, expr, when, size, explode, explode_outer, lit
import random
import string
import traceback
import json
from logging_utility import * 

# Helper function to fetch the Config Table
def fetchConfigDF(spark,config_table_name, config_id, conn_params):
    """
    Replace Below Code with query to return the config table as a dataframe from Azure SQL DB. 
    Config Table should have the same schema as dq_config_df
    """
    try:
        print(f"Starting to fetch config for dq_id: {config_id}")

        query = f"SELECT * FROM {config_table_name} WHERE dq_id = '{config_id}'"
        dq_data = execute_query_on_server(spark,query,conn_params)
        dq_config_df = dq_data.filter(col('is_active') == lit('TRUE'))

        """
        End of Code to fetch dq_config_df from Azure SQL DB
        """

        # Find the dq_config_id for the source table
        df_table_count = dq_config_df.select(col('table_name')).distinct().count()  

        if df_table_count == 0: 
            raise Exception("This dq_config_id is not mapped to any table.")

        if df_table_count > 1 : 
            raise Exception("This dq_config_id is mapped to more than 1 table.") 
        
        print(f"Fetched config for dq_id: {config_id}. Configs: {dq_config_df}")
        return dq_config_df
    
    except Exception as e:
        print(str(traceback.format_exc()))
        raise Exception()

def get_dq_summary(spark, df_valid, df_invalid):
    try:
        dq_summary = {}
        dq_summary["total_record_count"] = df_valid.count() + df_invalid.count()
        dq_summary["failed_record_count"] = df_invalid.count()
        if dq_summary["total_record_count"] > 0:
            dq_summary["failure_percentage"] =  (dq_summary["failed_record_count"] / dq_summary["total_record_count"] ) * 100
        else:
            dq_summary["failure_percentage"] = None
        
        check_sumary = df_invalid\
            .select(explode("dq_details")\
            .alias("dq_details"))\
            .select(col("dq_details")\
            .getItem(0).alias("checks"))\
            .groupBy("checks")\
            .count()\
            .collect()
        check_summary_dict = [row.asDict() for row in check_sumary]
        dq_summary["check_level_summary"] = check_summary_dict
        return dq_summary
    except Exception as e:
        print(str(traceback.format_exc()))
        raise Exception()

# Generate dq_checks dictionary using PySpark
def get_dq_check(spark,dq_config_df): 
    try:
        print("START OF DQ CHECKS")
        dq_checks = {}
        for row in dq_config_df.collect():
            dq_config_id,rule_id, _, table_name, col_name, _, _, rule_name, rule_val, _, _, _, _, _, _,_, _, _, _ = row
            
            
            if dq_config_id not in dq_checks:
                dq_checks[dq_config_id] = {}

            if rule_name == "invalid_primary_column":
                check = f"`{col_name}` IS NULL"
            elif rule_name == "data_check":
                if rule_val.startswith("Is Between"):
                    min_val, max_val = map(int, rule_val.split(" ")[-3::2])
                    check = f"`{col_name}` < {min_val} OR `{col_name}` > {max_val}"
                else:
                    # Handle other data check scenarios if needed
                    check = f"`{col_name}` {rule_val}"
            else:
                # Handle other rule types if needed
                check = None

            if check:
                # Use the same rule_id for each table with the same dq_config_id
                if rule_id not in dq_checks[dq_config_id]:
                    dq_checks[dq_config_id][str(rule_id)] = {"check": check, "col_name": col_name}
        
        print("END OF DQ CHECKS")
        return dq_checks
    except Exception as e:
        print(str(traceback.format_exc()))
        raise Exception()

# Helper function to generate unique numeric extension for temp table
def generate_unique_extension(length):
    """
    Purpose     :   Generate unique numeric extension
    Input       :   Length of extension
    Output      :   Return the Unique numeric extension 
    """
    return ''.join(random.choices(string.digits, k=length))

def data_quality_runner(spark,df,dq_checks):
    """
    Purpose     :   Perform all DQ Checks of a table 
    Input       :   a. Dataframe of the table
                    b. Dictionary of all DQ Checks of the table. 
    Output      :   a. Results of DQ Check. 
                    b. SQL Query of the DQ
                    c. DQ Summary 
    """
    try:
        print("START OF DATA QUALITY RUNNER")
        # Random name extension for temp view name
        temp_table_name = 'temp_view_' + generate_unique_extension(16)

        # Create a temporary view
        df.createOrReplaceTempView(temp_table_name)

        # Constructing when clause per dq dictionary
        when_clauses = []
        for dq_id in dq_checks:
            check = dq_checks[dq_id]["check"]
            col_name = dq_checks[dq_id]["col_name"]
            clause = "case when " + check + " then array('" + check + "', '" + col_name + "') else null end as dq_check_" +  dq_id
            when_clauses.append(clause)
        
        # print("when clause:",when_clauses)

        # Constructing sql query for DQ
        dq_query = f"""
            SELECT
                *,
                {', '.join(when_clauses)}
            FROM {temp_table_name}
        """
        # List of temp dq columns to be created
        dq_check_cols = [
            f"dq_check_{dq_id}"
            for dq_id in dq_checks
        ]
        print(dq_check_cols)

        # Execute the DQ Query
        dq_result_df = spark.sql(dq_query)
        print("after DF creation")
        # Formatting DQ result dataframe and removing temp dq columns
        dq_result_df_details = dq_result_df\
            .withColumn("dq_details", array(*[col(c) for c in dq_check_cols]))\
            .withColumn("dq_details", expr("FILTER(dq_details, element -> element IS NOT NULL)"))\
            .withColumn("dq_record_status", when(size(col("dq_details")) == 0, "valid").otherwise("invalid"))\
            .drop(*dq_check_cols)
        # print("end runner")
        print("END OF DATA QUALITY RUNNER")
        return dq_result_df_details, dq_query
    except Exception as e:
        print(str(traceback.format_exc()))
        raise Exception()

#Function Call
def execute_dq_checks(source_table_df, dq_config_id, config_table_name, conn_params, spark = None):
    try : 

        dq_config_df = fetchConfigDF(config_table_name, dq_config_id, conn_params)
        dq_checks = get_dq_check(dq_config_df)
        dq = next(iter(dq_checks.values()))
        print(f"DQ CHECKS VALUES: {dq}")
        df_out, dq_query = data_quality_runner(source_table_df, dq)

        return df_out,dq_query

    except Exception as e: 
        print(e)
