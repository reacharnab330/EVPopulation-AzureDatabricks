# Databricks notebook source
# Databricks Notebook: 03_data_transformation

from pyspark.sql.functions import col, explode
from pyspark.sql.types import IntegerType, StringType, DoubleType
import json

# Load configuration (use /dbfs/ prefix for Python file operations)
with open("/dbfs/mnt/data_dir/config/ev_config.json", "r") as f:
    CONFIG = json.load(f)

def flatten_data(df):
    """Flatten the nested 'data' array into individual rows."""
    return df.select(explode(col("data")).alias("vehicle_data")) \
        .select(
            col("vehicle_data")[8].alias("vin_1_10").cast(StringType()),
            col("vehicle_data")[9].alias("county").cast(StringType()),
            col("vehicle_data")[10].alias("city").cast(StringType()),
            col("vehicle_data")[11].alias("state").cast(StringType()),
            col("vehicle_data")[12].alias("postal_code").cast(IntegerType()),
            col("vehicle_data")[13].alias("model_year").cast(IntegerType()),
            col("vehicle_data")[14].alias("make").cast(StringType()),
            col("vehicle_data")[15].alias("model").cast(StringType()),
            col("vehicle_data")[16].alias("electric_vehicle_type").cast(StringType()),
            col("vehicle_data")[17].alias("cafv_eligibility").cast(StringType()),
            col("vehicle_data")[18].alias("electric_range").cast(IntegerType()),
            col("vehicle_data")[19].alias("base_msrp").cast(DoubleType()),
            col("vehicle_data")[20].alias("legislative_district").cast(IntegerType()),
            col("vehicle_data")[21].alias("dol_vehicle_id").cast(StringType()),
            col("vehicle_data")[22].alias("vehicle_location").cast(StringType()),
            col("vehicle_data")[23].alias("electric_utility").cast(StringType()),
            col("vehicle_data")[24].alias("2020_census_tract").cast(StringType())
        )

#def parse_columns(df, schema_config):
#    """Parse the flattened data array and select only schema-defined columns."""
#    # Map the data array indices to schema column names
#    selected_columns = [
#        col("data").getItem(i).alias(field_name)
#        for i, field_name in enumerate(schema_config["columns"])
#    ]
#    return df.select(selected_columns)

def extract_approvals(metadata_df):
    """Extract and transform approval details from metadata."""
    return metadata_df.select(explode(col("approvals")).alias("approval"))\
                     .select(
                         col("approval.reviewedAt").alias("reviewed_at"),
                         col("approval.state"),
                         col("approval.submitter.id").alias("submitter_id"),
                         col("approval.submitter.displayName").alias("submitter_name")
                     )

# Load raw data from bronze layer
data_df = spark.read.format("delta").load(f"{CONFIG['bronze_path']}ev_data")
metadata_df = spark.read.format("delta").load(f"{CONFIG['bronze_path']}ev_metadata")

# Transform data with error handling
try:
    # Transform ev_data and filter to schema columns
    flat_df = flatten_data(data_df)
    #parsed_df = parse_columns(flat_df, CONFIG["schema"])

    # Transform ev_metadata into approvals
    approvals_df = extract_approvals(metadata_df)

    # Save transformed data to silver layer as Delta files without table registration
    flat_df.write.format("delta")\
             .mode("overwrite")\
             .option("mergeSchema", "true")\
             .option("delta.columnMapping.mode", "name")\
             .save(f"{CONFIG['silver_path']}ev_population")

    approvals_df.write.format("delta")\
                .mode("overwrite")\
                .option("mergeSchema", "true")\
                .option("delta.columnMapping.mode", "name")\
                .save(f"{CONFIG['silver_path']}ev_approvals")

    print("Data transformation completed and saved as Delta files in silver layer")
    #display(flat_df.limit(5))
    #display(approvals_df.limit(5))

except Exception as e:
    print(f"Transformation failed: {str(e)}")
    raise  # Re-raise the exception to halt execution and allow debugging