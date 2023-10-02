# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Setup Assignment
# MAGIC This notebook is the basis for the execution portion of the Databricks setup assignment.  This will involve connecting to your storage account to execute a couple of pre-written basic commands.  Then you will a simple column add of your own and save the results.

# COMMAND ----------

# The first step is to connect to your storage account after setting up a Databricks secret scope and a key vault.  Follow the PDF walkthrough to get this set up.
# You can use the storage account from the first assignment (GitHub and Azure) for this assignment.

# You'll need to replace these values with the specifics for your setup.

storage_end_point = "assign1storage.dfs.core.windows.net" 
my_scope = "MarchMadnessScope"
my_key = "assign1-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

# Replace the container name (assign-1-blob) and storage account name (assign1storage) in the uri.
uri = "abfss://assign-1-blob@assign1storage.dfs.core.windows.net/"


# COMMAND ----------

# Read the data file from the storage account.  This the same datafile used in assignment 1.  It is also available in the InputData folder of this assignment's repo.
sp_df = spark.read.csv(uri+'SandP500Daily.csv', header=True)
 
display(sp_df)

# COMMAND ----------

# Create new column with the range for the day.
sp_range_df = sp_df.withColumn('Range', sp_df.High - sp_df.Low)

display(sp_range_df)

# COMMAND ----------

# Save this range file to a single CSV.  Use coalesce to output it to a single file.
sp_range_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/Range")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Your coding part starts here.  
# MAGIC Fill in the PySpark in the following notebook cells.
# MAGIC

# COMMAND ----------

# Use the range from the previous cells to find the percent change for each day.  Use the Open column for the denominator.
# Sort the dataset descending based on the percent change (High to Low).



# COMMAND ----------

# Save the file to a single CSV file to your storage account to a single CSV file in the location output/PercentChange.


