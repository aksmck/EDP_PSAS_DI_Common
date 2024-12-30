# ################################################## Module Information ################################################
#   Module Name         :   Logging_Utility 
#   Purpose             :   This module inputs/updates logs in logging master table
#   Input               :   logging_master table 
#   Output              :   Return status SUCCESS/FAILED
#   Pre-requisites      :   Logging_master Table has to be created and populated
#   Last changed on     :   15 Jan 2024
#   Last changed by     :   Prakhar Chauhan
#   Reason for change   :   
# ######################################################################################################################


#Notebook Constants
STATUS = "status"
SUCCESS = "SUCCESS"
ERROR = "error"


#,Python Imports

#Create JDBC URL
# Function to create JDBC URL
def create_jdbc_url(database_host,database_port, database_name):
    return f"jdbc:sqlserver://{database_host}:{database_port};database={database_name}"

def insert_data_to_server(spark,values_to_insert, configs):
    # Set up Params
    conn_params = configs
    database_host = conn_params["database_host"]
    database_port = conn_params["database_port"]
    database_name = conn_params["database_name"]
    table = conn_params["table"]
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    user = conn_params["user"]
    password = conn_params["password"]
    url = create_jdbc_url(database_host,database_port,database_name)

    try:
        data = [values_to_insert] 
        # creating a dataframe of input key/value pairs 
        dataframe = spark.createDataFrame(data)
        (dataframe.write
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .mode("append")
        .save()
        )
        
        # Return the status
        return {"STATUS": "SUCCESS"}
    except Exception as e:
        # Handle exceptions
        print("An error occurred:", str(e))

#Execute Query on Server
def execute_query_on_server(spark,query, configs):
    # Set up Params
    conn_params = configs
    database_host = conn_params["database_host"]
    database_port = conn_params["database_port"]
    database_name = conn_params["database_name"]
    user = conn_params["user"]
    password = conn_params["password"]
    table = conn_params["table"]
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url = create_jdbc_url(database_host,database_port,database_name)

    try:
        # Read data from table
        df_data = (spark.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", f"({query}) AS temp_table")
        .option("user", user)
        .option("password", password)
        .load()
        )
        
        # Return the DataFrame
        return df_data
    except Exception as e:
        # Handle exceptions
        print("An error occurred:", str(e))
