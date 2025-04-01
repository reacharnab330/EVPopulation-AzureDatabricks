# Databricks notebook source
# MAGIC %md
# MAGIC 05_export_to_Snowflake

# COMMAND ----------

#Snowflake Account Config

snowflake_options = {
    "sfURL": "<Snowflake URL>",
    "sfUser": "<user id>",
    "sfPassword": "<password>",
    "sfDatabase": "EVDATA_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN"
}

# COMMAND ----------



# COMMAND ----------


import json
# Load configuration (use /dbfs/ prefix for Python file operations)
with open("/dbfs/mnt/data_dir/config/ev_config.json", "r") as f:
    CONFIG = json.load(f)

def export_to_snowflake(df, table_name, options):
    df.write.format("snowflake") \
        .options(**options) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()

# Read the gold data
ev_population_clean_df = spark.read.format("delta").load(f"{CONFIG['gold_path']}ev_population_clean")

# Export to Snowflake
export_to_snowflake(ev_population_clean_df, "EV_POPULATION", snowflake_options)