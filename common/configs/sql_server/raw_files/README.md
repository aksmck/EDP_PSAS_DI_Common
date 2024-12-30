## Raw data for SQL server tables: 

  * The raw data present in the form of Excel sheet are used to create new records or/and update new records in the SQL server table
  * These Excel sheets are pushed to ADLS location and the path needs to be configured in pipeline Misc- pl_ingest_data_in_pipeline_configs. <i> Please find more details for the pipeline [here](/common/ARMTemplates/README.md) </i>.
  * On running the above mentioned pipeline, this data is loaded in the SQL server table 
