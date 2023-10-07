# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Setup Assignment
# MAGIC This notebook is the basis for the execution portion of the Databricks setup assignment.  This will involve connecting to your storage account to execute a couple of pre-written basic commands.  Then you will a simple column add of your own and save the results.
import dbutils
from Tools.scripts.dutree import display
from pyspark.shell import spark

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
datafile = spark.read.csv(uri+'SandP500Daily.csv', header=True)
 
display(datafile)

# Creating the new column with the range for the day.
range_column_datafile = datafile.withColumn('Range', datafile.High - datafile.Low)

display(range_column_datafile)

# Saving this range file to a single CSV. Using coalesce to output it to a single file.
range_column_datafile.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/Range")

# Using the range from the previous cells to find the percent change for each day.
range_datafile_percent_change = range_column_datafile.withColumn('Percent Change', (range_column_datafile.Range/range_column_datafile.Open) * 100)

# Sorting the dataset descending based on the percent change (High to Low).
range_datafile_sorted = range_datafile_percent_change.orderBy("Percent Change", ascending=False)

display(range_datafile_sorted)

# Saving the file to a single CSV file to storage account in the location output/PercentChange.
range_datafile_sorted.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/PercentChange")

