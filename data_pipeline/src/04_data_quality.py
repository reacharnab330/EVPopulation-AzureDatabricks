# Databricks notebook source
# MAGIC %md
# MAGIC 04_data_quality

# COMMAND ----------

from pyspark.sql.functions import col, length
import json
from datetime import datetime

# Load configuration (use /dbfs/ prefix for Python file operations)
with open("/dbfs/mnt/data_dir/config/ev_config.json", "r") as f:
    CONFIG = json.load(f)

def perform_quality_checks(df):
    """Perform quality checks and filter out invalid records."""
    # Define quality check conditions
    current_year = datetime.now().year + 1  # e.g., 2026 as of March 27, 2025
    
    # Conditions for valid records
    valid_conditions = (
        # Null checks
        col("vin_1_10").isNotNull() &
        col("model").isNotNull() &
        # Range checks
        (col("electric_range") > 0) & (col("electric_range") < 1000) &
        (col("base_msrp") >= 0) & (col("base_msrp") < 1000000) &
        (col("model_year") >= 1886) & (col("model_year") <= current_year)
    )

    # Filter valid records
    valid_df = df.filter(valid_conditions)
    
    # Create quality report
    quality_report = {}

    # 1. Null checks
    null_check_columns = ["vin_1_10", "model"]
    quality_report["null_counts"] = {
        col_name: df.filter(col(col_name).isNull()).count()
        for col_name in null_check_columns
    }

    # 2. Duplicate checks on vin_1_10
    quality_report["duplicate_count_vin_1_10"] = df.groupBy("vin_1_10")\
                                                  .count()\
                                                  .filter(col("count") > 1)\
                                                  .count()

    # 3. Range checks
    quality_report["invalid_electric_range"] = df.filter(
        (col("electric_range") <= 0) | (col("electric_range") >= 1000)
    ).count()
    quality_report["invalid_base_msrp"] = df.filter(
        (col("base_msrp") < 0) | (col("base_msrp") >= 1000000)
    ).count()
    quality_report["invalid_model_year"] = df.filter(
        (col("model_year") < 1886) | (col("model_year") > current_year)
    ).count()

    # 4. Additional info: Total records and valid records
    quality_report["total_records"] = df.count()
    quality_report["valid_records"] = valid_df.count()
    
    # Optional: Count distinct state codes
    quality_report["distinct_state_count"] = valid_df.select("state").distinct().count()
    quality_report["distinct_states"] = [row["state"] for row in valid_df.select("state").distinct().collect()]

    return quality_report, valid_df

# Load transformed data from silver layer
population_df = spark.read.format("delta").load(f"{CONFIG['silver_path']}ev_population")

# Perform quality checks and filter data
try:
    # Perform quality checks and get valid records
    quality_report, valid_df = perform_quality_checks(population_df)
    #display(quality_report)

    # Print additional info
    print(f"Total records: {quality_report['total_records']}")
    print(f"Valid records: {quality_report['valid_records']}")
    print(f"Number of distinct state codes in valid data: {quality_report['distinct_state_count']}")
    print(f"List of distinct state codes in valid data: {quality_report['distinct_states']}")

    # Write valid records to gold layer
    if quality_report["valid_records"] > 0:
        valid_df.write.format("delta")\
                .mode("overwrite")\
                .option("delta.columnMapping.mode", "name")\
                .save(f"{CONFIG['gold_path']}ev_population_clean")
        print(f"Valid data written to gold layer at {CONFIG['gold_path']}ev_population_clean")
    else:
        print("No valid records found after quality checks; no data written to gold layer.")

except Exception as e:
    print(f"Quality check process failed: {str(e)}")
    raise  # Re-raise the exception to halt execution and allow debugging