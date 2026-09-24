#!/usr/bin/env python
# coding: utf-8

# ## bronze_ingestion_fabric
# 
# New notebook

# 
# 
# ## Purpose
# The Bronze layer preserves source data with minimal transformation.
# 

# ## 1. Read raw source data
# 
# The source CSV is stored in Azure Data Lake Storage Gen2 and exposed in the Lakehouse through the `employees_raw` shortcut.
# 
# Schema inference is disabled intentionally so that Bronze preserves source values as strings.

# In[1]:


raw_path = "Files/employees_raw/employees.csv"

bronze_df = (
    spark.read
    .option("header", True)
    .option("inferSchema",False)
    .csv(raw_path)
)


# ## 2. Validate ingestion
# 
# Check the record count and schema to confirm that the full source batch was ingested correctly.

# In[2]:


bronze_df.count()


# In[3]:


bronze_df.printSchema()


# In[4]:


bronze_df.show(10, truncate=False)


# ## 3. Add ingestion metadata
# 
# Add technical columns used for lineage and auditing:
# - `_ingested_at` — ingestion timestamp
# - `_source_file` — source file name

# In[5]:


from pyspark.sql.functions import current_timestamp, lit

bronze_df = (
    bronze_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("employees.csv"))
)


# In[6]:


bronze_df.select(
    "employee_id",
    "_ingested_at",
    "_source_file"
).show(5, truncate=False)


# ## 4. Write Bronze Delta table
# 
# Persist the raw dataset as a Delta table in the Bronze Lakehouse.
# 
# No business validation or cleansing is performed in this layer.

# In[7]:


(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dbo.employees_bronze")
)


# ## 5. Validate Bronze Delta table
# 
# Read the persisted Bronze Delta table and verify that the record count matches the source batch.

# In[8]:


bronze_table_df = spark.table("dbo.employees_bronze")

bronze_table_df.count()


# ## Bronze ingestion completed
# 
# The source batch was successfully ingested from Azure Data Lake Storage Gen2
# and persisted as a Delta table in Microsoft Fabric.
# 
# - Source records: 1000
# - Bronze records: 1000
# - Target table: `dbo.employees_bronze`
# - Business cleansing: none
