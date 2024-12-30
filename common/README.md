## landing_to_bronze_ingestion 

The code provided is a Databricks notebook that performs the ingestion of data from a source file to a target table. Here is the flow of the code: 
* Importing Libraries/Functions: The necessary libraries and functions are imported. These include json, datetime, pyspark.sql.functions, pyspark.sql.types, and a custom logging_utility module. 
* Notebook Constants: Some constants used throughout the notebook are defined. These include keys for status, result, failed, succeeded, error, execution status, end time, and layer. Additionally, there are some housekeeping column names defined. 
* Declaring variables: Various variables are declared, including those related to the Databricks context, logging details, application variables, and Databricks table parameters. 
* Get target table details: This code block defines a function (commented out) that retrieves the details of the target table. It splits the target_table_identifier variable to get the delta_catalog, delta_database, and delta_table. The function returns the target table identifier. 
* Prepare logging info: This code block defines two functions. The first function, prepare_sql_connection_info, prepares the SQL connection parameters for logging. It retrieves the necessary information from the notebook widgets and secret keys. The second function, prepare_logging_info, prepares the logging info by populating a dictionary with various parameters such as source table, target table, start time, ADF run ID, ADF job ID, layer, environment, process name, interface name, notebook location, Databricks job ID, and Databricks run ID. 
* Source to target data copy: This code block defines a function called source_to_target_data_copy. This function performs the actual data copy from the source file to the target table. It uses the readStream and writeStream methods of Spark to read the data from the source file and write it to the target table. It also includes additional operations such as dropping unnecessary columns and adding housekeeping columns. The function waits for the streaming query to complete before returning. 
* Main method: The main method is the entry point of the notebook. It calls the functions in the defined order to perform the data ingestion process. It prints the source file path and starts the data ingestion process. 

Each code block serves a specific purpose in the data ingestion process, such as importing libraries, setting constants, declaring variables, retrieving target table details, preparing logging information, performing the data copy, and executing the main method. 
The code includes error handling and logging of execution status and other parameters to a logging table using the insert_data_to_server function. 

## bronze_to_silver_ingestion 

The code performs the following steps: 
* Import necessary libraries and functions. 
* Declare variables and extract information from the Databricks context and input parameters. 
* Create a function to generate MD5 hash for the data. 
* Create a function to prepare SQL connection parameters for the logging table. 
* Create a function to perform data copy from the source table to the target table. 
* The function reads the source table as a stream. 
* If DQ checks are enabled, it applies DQ checks on the source table to identify valid and invalid records. 
* It creates an MD5 hash column if specified in the input parameters. 
* It writes the valid records to the target table. 
* It writes the invalid records to a separate table. 
* It generates a DQ summary based on the valid and invalid records. 
* It logs the source and target table counts. 
* In the main code: 
  * It prepares SQL connection parameters for the logging table. 
  * If successful, prepares logging information.
  * If successful, performs data copy from the source table to the target table.
    
## silver_to_gold_ingestion 

* The code you provided is a Databricks notebook that performs the following tasks: 
* Import necessary functions and libraries.
* Declare constants and variables.
* Prepare SQL connection information for logging.
* Prepare logging information.
* Define a function to process each batch of data.
* Define a function to load data from the source table to the target table.
* Start the data load process.
* Perform logging of the execution status.
* Here is the flow of the code:
    * Import necessary functions and libraries.
    * Declare constants and variables.
    * Prepare SQL connection information for logging.
    * Prepare logging information. 
    * Define a function to process each batch of data.
    * Define a function to load data from the source table to the target table by streaming data from the source table, performing DML operations on the data, and writing the resulting data to the target table.
    * Start the data load process by calling the function defined in step 6.
    * Perform logging of the execution status by inserting the logging information to a logging table. 

## dml_executor_utility 

The code is a Databricks notebook that performs the following operations: 
* Imports necessary libraries.
* Declares notebook constants such as status keys, result keys, failed and success keys, and error keys.
* Declares variables and initializes them with values from Databricks widgets and contexts.
* Prepares SQL connection information for logging.
* Prepares logging information.
* Defines a function to prepare SQL connection information for logging.
* Defines a function to prepare logging information.
* Defines a function to execute SQL queries.
* The main function initializes the variables, prepares SQL connection information, prepares logging information, and triggers the execution of SQL queries.
* If any process fails, an exception is raised.
* The flow of the code is as follows:
    * Imports necessary libraries.
    * Declares notebook constants.
    * Initializes variables and prepares SQL connection information.
    * Prepares logging information. 
    * Defines functions to prepare SQL connection information and logging information. 
    * Defines a function to execute SQL queries. 
    * The main function is called, which initializes the variables, prepares SQL connection information, prepares logging information, and triggers the execution of SQL queries. 
    * If any process fails, an exception is raised. 

## dq_utility 

The code is a Databricks notebook that performs data quality (DQ) checks on a table. Here's how the code works: 
* The code imports necessary libraries and modules.
* There is a helper function, fetchConfigDF, which fetches the DQ configuration table from Azure SQL DB based on the provided config ID.
* There is a get_dq_summary function that takes the valid and invalid dataframes as input and calculates the DQ summary metrics such as total record count, failed record count, and failure percentage.
* The code defines a function, get_dq_check, that generates the DQ checks dictionary based on the provided DQ configuration dataframe. It iterates over each row of the dataframe and constructs the DQ check for each rule type and rule value.
* There is a helper function, generate_unique_extension, that generates a unique numeric extension of a specified length.
* The data_quality_runner function performs the DQ checks on the given dataframe using the generated DQ checks dictionary. It creates a temporary view of the dataframe, constructs the SQL query for DQ checks, and executes the query to obtain a dataframe with DQ results.
* Lastly, there is a function execute_dq_checks that orchestrates the execution of DQ checks. It fetches the DQ configuration dataframe, generates the DQ checks dictionary, and calls the data_quality_runner function to perform the DQ checks on the provided source table dataframe.
* The flow of the code is as follows: 
Import necessary libraries and modules. 
  * Define helper functions. 
  * Define the get_dq_summary function. 
  * Define the get_dq_check function. 
  * Define the data_quality_runner function. 
  * Define the execute_dq_checks function. 
  * Call the execute_dq_checks function to perform DQ checks on the source table dataframe. 
  * Print the DQ checks values and return the DQ result dataframe and the DQ query. 

## logging_utility 

The code is a Logging Utility module that handles logging functionality in a data pipeline. Here's how the code works:
* The code defines a function, create_jdbc_url, that creates a JDBC URL using the provided host, port, and database name. 
* There is a function, insert_data_to_server, that inserts data into a SQL Server table using the JDBC connection. It takes the Spark session, values to insert, and connection parameters as input. It creates a DataFrame from the input values and writes it to the SQL Server table using the JDBC format and provided connection parameters. 
* There is a function, execute_query_on_server, that executes a query on the SQL Server using the JDBC connection. It takes the Spark session, query, and connection parameters as input. It reads the data from the SQL Server table using the JDBC format and provided connection parameters and returns the DataFrame with the query result. 
* The flow of the code is as follows: 
  * Define the create_jdbc_url function. 
  * Define the insert_data_to_server function. 
  * Define the execute_query_on_server function. 
  * Use the insert_data_to_server function to insert data into the logging master table. 
  * Use the execute_query_on_server function to execute queries on the logging master table. 

## Reconciliation 

The flow of code in the given code snippet is as follows: 
* Importing necessary libraries and functions. 
* Declaring variables. 
* Defining a method to create dataframes for SQL database tables. 
* Loading SQL database tables into dataframes. 
* Creating temp views for each table to be used for reconciliation. 
* Loading recon queries from pipeline configuration based on the input process name. 
* Performing reconciliation for each table in the process. 
* Creating a schema for the recon summary table to store the reconciliation results. 
* Creating a dataframe with the reconciliation summary data. 
* Creating a view for the recon summary dataframe. 
* Displaying the recon summary table. 
* Preparing the reconciliation summary in HTML format with styling. 
* Displaying the formatted HTML table with reconciliation summary. 
* Raising an exception in case of any error and returning the reconciliation summary as a response. 

The code performs reconciliation by comparing the count of records for each table in different layers (source, landing, bronze, silver, gold) and generates a summary of the reconciliation results in an email. 

 
