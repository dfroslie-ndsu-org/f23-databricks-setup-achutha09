# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Setup Assignment
# MAGIC This notebook is the basis for the execution portion of the Databricks setup assignment.  This will involve connecting to your storage account to execute a couple of pre-written basic commands.  Then you will a simple column add of your own and save the results.

# COMMAND ----------

# Connecting to the storage account using the secret scope and keyVault

storage_end_point = "achuthastorage.dfs.core.windows.net"
my_scope = "databricksSecretScope"
my_key = "storage-keyvault"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

uri = "abfss://assign1@achuthastorage.dfs.core.windows.net/"

# Reading the data file from the storage account.  
sp_df = spark.read.csv(uri+'SandP500Daily.csv', header=True)
 
display(sp_df)

# Creating the new column with the range for the day.
sp_range_df = sp_df.withColumn('Range', sp_df.High - sp_df.Low)

display(sp_range_df)

# Saving this range file to a single CSV. Using coalesce to output it to a single file.
sp_range_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/Range")

# Using the range from the previous cells to find the percent change for each day.
sp_range_df_percent_change = sp_range_df.withColumn('Percent Change', (sp_range_df.Range/sp_range_df.Open) * 100)

# Sorting the dataset descending based on the percent change (High to Low).
sp_range_df_sorted = sp_range_df_percent_change.orderBy("Percent Change", ascending=False)

display(sp_range_df_sorted)

# Saving the file to a single CSV file to storage account in the location output/PercentChange.
sp_range_df_sorted.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/PercentChange")

