## FDP Pipeline Design: 

* ### Misc- pl_create_table_utility:
    * This pipeline is used whenever a new table needs to be created in Databricks catalog.
    * The pipeline starts with a Lookup Config activity, which reads the pipeline config table stored in SQL Server and takes process name as input.
    * Post reading the pipeline config table, there is a for each activity encapsulating the create table utility which creates the table for all the entries read for the input process name from pipeline config.
    * The create table utility is a databrcks utility which takes 3 inputs- table name, table storage path and table ddl file path and creates the table for one given layer in one run i.e., bronze/silver/gold . 

* ### Misc- pl_ingest_data_in_pipeline_configs: 
    * This pipeline is used to create/update bulk entries within pipeline configs table on SQL server.  
    * This pipeline has one copy data activity which has its source configured as ADLS path and sink configured as SQL server table. 
    * When we run this pipeline, the copy data activity reads Excel file from ADLS path and upserts the data in configured SQL server table matching on the primary key column. <i> Please find more details regarding the Excel data [here](/common/configs/sql_server/raw_files/README.md) </i>

* ### Misc- pl_update_pipeline_configs:  
    * This pipeline consists of a stored procedure activity and is used to update pipeline config entry for a given input ID.
    * When we run the pipeline, we need to provide 3 input values- key, value and ID. Key is the column name for which the data needs to be changed, value is the new data that needs to be updated for the key and ID is the record ID on table.

* ### Landing pipeline- pl_<process_name>_landing:
    * This pipeline is used to ingest data from source to landing layer for a given input process name.
    * This pipeline has a lookup activity which looks for the table name in pipeline_configs table.
    * For the table name, it looks for the connection in connection control table.
    * For the fetched connection details, it start the copy activity in a for each loop which copies the data from source system to ADLS landing container.
    * After completion of the copy activity, a log entry is created on audit table in SQL server with the copy activity's details.
    * Upon completion of the pipeline a email is send to respective recipient from configured logic app.

* ### Bronze pipeline- pl_<process_name>_bronze:
    * This pipeline is used to ingest data from landing to bronze layer for a given input process name.
    * This pipeline has a lookup activity which looks for the table name in pipeline_configs table.
    * For the tables found in pipeline configs, landing to bronze ingestion starts in a for each loop.
    * Upon completion of bronze ingestion mail is send to respective recipient from configured logic app.

* ### Silver pipeline- pl_<process_name>_silver:
    * Lookup activity is used to fetch configurations needed to start the pipeline. Configurations are fetched by a query that we pass in the lookup activity. Pipeline related configurations are stored in the config table created in SQL server that is hosted in AWS RDS machine.
    * A ForEach loop is executed following the lookup activity to iterate over all the childitems in the output of the previous activity.
    * Inside ForEach loop there is silver layer ingestion databricks notebook activity that links to databricks notebook by the given path in the activity. In Case of failure an email will be sent to the business stakeholders and developers using the Web activity and the pipeline will be failed using the Fail Activity by a Fail message and Error code.
    * Outside the ForEach loop if the iteration fails an email will be sent to the business stakeholders and developers using the Web activity and the pipeline will be failed using the Fail Activity by a Fail message and Error code.
    * In case of Success an email will be sent to the business stakeholders and developers using the Web activity to notify the successfull execution of the pipeline flow.
 
* ### Gold pipeline- pl_<process_name>_gold:
    * Lookup activity is used to fetch configurations needed to start the pipeline. Configurations are fetched by a query that we pass in the lookup activity. Pipeline related configurations are stored in the config table created in SQL server that is hosted in AWS RDS machine.
    * A ForEach loop is executed following the lookup activity to iterate over all the childitems in the output of the previous activity.
    * Inside ForEach loop there is gold layer ingestion databricks notebook activity that links to databricks notebook by the given path in the activity. In case of successfull execution of the gold layer ingestion notebook the gold last updated CDC version is fetched and updated. Using a Stored Procedure activity the version is then set for the particular table.
    * Following this an If Condition activity is executed  this checks for particular q tables using another databricks utility notebook. Max Start Date and End Date is fetched start date is updated using a stored procedure.
    * Following this an If Condition activity is executed on Completion of previous flow this checks for particular q tables and failure/success status of the previous activity (For deltailed logic for the If Condition do refer the Azure Expression). If the previous activity is failed the pipeline will be failed using the Fail Activity by a Fail message and Error code.
    * In case of failure of the gold layer ingestion notebook an email will be sent to the business stakeholders and developers using the Web activity.
    * Outside the ForEach loop if the iteration fails an email will be sent to the business stakeholders and developers using the Web activity and the pipeline will be failed using the Fail Activity by a Fail message and Error code.
    * In case of Success an email will be sent to the business stakeholders and developers using the Web activity to notify the successfull execution of the pipeline flow.
