# Databricks notebook source
# MAGIC %md
# MAGIC 01_config_setup

# COMMAND ----------

#Storage Account details ***SECRET***

# Storage account details
storage_account_name = "<storage account name>"
container_name = "<container name>"
access_key = dbutils.secrets.get(scope="akv", key="access-key")


# COMMAND ----------

##Set up the mount point 

# Define the source and mount point
source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
mount_point = "/mnt/data_dir"

# Unmount the storage account if already mounted
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Mount the storage account
dbutils.fs.mount(
  source = source,
  mount_point = mount_point,
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
)

# List the contents of the new mount point
#dbutils.fs.ls(mount_point)

# COMMAND ----------

import json

# Configuration dictionary
CONFIG = {
    "storage_path": "/mnt/data_dir/srcdata/",
    "bronze_path": "/mnt/data_dir/bronze_delta/",
    "silver_path": "/mnt/data_dir/silver_delta/",
    "gold_path": "/mnt/data_dir/gold_delta/",
    "input_filename": "ElectricVehiclePopulationData.json"
}



# Save config to DBFS for other notebooks
dbutils.fs.put("/mnt/data_dir/config/ev_config.json", json.dumps(CONFIG), overwrite=True)
print("Configuration setup completed")