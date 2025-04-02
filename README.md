# EVPopulation-AzureDatabricks
A modern ELT data pipeline for processing the JSON data set ElectricVehiclePopulationData.json using PySpark on Azure Databricks. 
The code also inlcudes the components for exporting the processed data from Azure databricks to Snowflake and then creating visualization using Streamlit in Snowflake (SiS).
In this data pipeline, it is assumed that the source data file is in an Azure Blob storage.

# Architecture 

<img src="https://github.com/reacharnab330/EVPopulation-AzureDatabricks/blob/main/solution_arch_adb.PNG">

# Project Overview
The project has two components :
1. Loading and processing and storing the records JSON file from Azure Blob using Azure Databricks using modern data lakehouse architecture in Pyspark
2. Exporting the processed and validated dataset to Snowflake and creating visualization using SiS

# Data Model

Folloiwng the multilayer datalake architecture the following datasets are created in different layers :

Bronze    : ev_raw, ev_metadata, ev_data ;
Silver    : ev_population, ev_approvals ;
Gold      : ev_population_clean ;
Snowflake : ev_population ;

# How to use this project 

1. Clone this repo
2. data_pipeline/src contains the databricks pyspark notebook files that need to be placed in Azure Databricks notebook
3. data_pipeline/data contains the JSON format source data file that need to be placed in Azure Blob storage
4. streamlit_app/src contains the source code for the Streamlit in Snowflake (SiS) app that need to be setup in Snowflake

# How to run the project 

In databricks , 00_orchestrator_notebook is the starting point for the data pipeline which in turn calls the other notebooks for ingestion, processing , validation and export to Snowflake.
After successful export to Snowflake, run the Streamlit App in snowflake to experience the visualization.
