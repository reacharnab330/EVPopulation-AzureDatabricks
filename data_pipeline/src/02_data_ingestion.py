# Databricks notebook source
# MAGIC %md
# MAGIC 02_data_ingestion
# MAGIC
# MAGIC

# COMMAND ----------

#dbutils.notebook.run("01_config_setup", 300)

# COMMAND ----------

from pyspark.sql.functions import col
import json

# Load configuration (use /dbfs/ prefix for Python file operations)
with open("/dbfs/mnt/data_dir/config/ev_config.json", "r") as f:
    CONFIG = json.load(f)

def read_json_from_cloud(storage_path, filename):
    try:
        # Read JSON file dynamically based on provided filename
        raw_df = spark.read.option("multiline", "true").option("inferSchema", "true").json(f"{storage_path}{filename}")
        return raw_df
    except Exception as e:
        print(f"Error reading JSON file {filename}: {str(e)}")
        raise


def extract_metadata(raw_df):
    metadata_df = raw_df.select("meta.view.*")
    #display(metadata_df)
    return raw_df.select("meta.view.*")

def extract_data(raw_df):
    data_df = raw_df.select("data")
    #display(data_df)
    return raw_df.select("data")

# Execute ingestion using the filename from CONFIG
raw_df = read_json_from_cloud(CONFIG["storage_path"], CONFIG["input_filename"])
metadata_df = extract_metadata(raw_df)
data_df = extract_data(raw_df)

# Save intermediate results using bronze_path with column mapping enabled
raw_df.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(f"{CONFIG['bronze_path']}ev_raw")
metadata_df.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(f"{CONFIG['bronze_path']}ev_metadata")
data_df.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(f"{CONFIG['bronze_path']}ev_data")

print(f"Data ingestion completed for file: {CONFIG['input_filename']}")

#display(raw_df.limit(5))
#display(metadata_df.limit(5))
#display(data_df.limit(5))
