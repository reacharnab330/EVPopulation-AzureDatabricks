# Databricks notebook source
# Databricks Notebook: 06_main_pipeline

from datetime import datetime
import time

# Run all notebooks in sequence
try:
    start_time = time.time()
    print(f"Pipeline started at {datetime.now()}")
    
    dbutils.notebook.run("01_config_setup", 300)
    dbutils.notebook.run("02_data_ingestion", 300)
    dbutils.notebook.run("03_data_transformation", 300)
    dbutils.notebook.run("04_data_quality", 300)
    dbutils.notebook.run("05_export_to_Snowflake", 300)
    
    #print(f"Pipeline completed successfully!!")
    # Calculate and print execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Pipeline execution completed in {execution_time:.2f} seconds")
    
    
except Exception as e:
    print(f"Pipeline failed: {str(e)}")
    raise