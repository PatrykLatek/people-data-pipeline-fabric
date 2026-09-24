#!/usr/bin/env python
# coding: utf-8

# ## silver_validation_fabric
# 
# New notebook

# # Silver Validation
# 
# Validates, cleans and deduplicates employee data from the Bronze layer.
# 
# Source:
# `people_data_bronze.dbo.employees_bronze`
# 
# Target:
# `people_data_silver`

# ## Load Bronze Data

# In[77]:


bronze_df = spark.table(
    "people_data_bronze.dbo.employees_bronze"
)

display(bronze_df)


# In[78]:


bronze_df.printSchema()


# In[79]:


bronze_df.count()


# ## Type Casting

# In[80]:


from pyspark.sql.functions import col

silver_df = (
    bronze_df
    .withColumn("employee_id", col("employee_id").cast("int"))
    .withColumn("department_id", col("department_id").cast("int"))
    .withColumn("salary", col("salary").cast("int"))
)


# In[81]:


silver_df.select(
    "employee_id",
    "department_id",
    "salary"
).printSchema()


# ## Date Parsing
# 

# In[82]:


silver_df.select("hire_date").show(500, truncate=False)


# In[83]:


from pyspark.sql.functions import try_to_timestamp, lit

silver_df = silver_df.withColumn(
    "hire_date_parsed",
    try_to_timestamp(
        col("hire_date"),
        lit("yyyy-MM-dd HH:mm:ss")
    )
)


# In[84]:


silver_df.select(
    "hire_date",
    "hire_date_parsed"
).show(50, truncate=False)


# In[85]:


silver_df = silver_df.withColumn(
    "updated_at",
    try_to_timestamp(
        col("updated_at"),
        lit("yyyy-MM-dd HH:mm:ss")
    )
)


# In[86]:


silver_df.select("updated_at").show(100, truncate=False)


# In[87]:


silver_df.select(
    "updated_at"
).printSchema()


# ## Data Quality Checks
# 

# In[88]:


from pyspark.sql.functions import when

silver_df = silver_df.withColumn(
    "salary_error",
    when(
        col("salary") <= 0,
        "INVALID_SALARY"
    ).otherwise(None)
)


# In[89]:


silver_df.select(
    "employee_id",
    "salary",
    "salary_error"
).show(20, truncate=False)


# In[90]:


silver_df = silver_df.withColumn(
    "status_error",
    when(
        col("status").isNull()
        | ~col("status").isin("ACTIVE", "INACTIVE"),
        "INVALID_STATUS"
    ).otherwise(None)
)


# In[91]:


display(silver_df)


# In[92]:


silver_df.select(
    "employee_id",
    "status",
    "status_error"
).show(30, truncate=False)


# In[93]:


from pyspark.sql.functions import when, trim

silver_df = silver_df.withColumn(
    "country_error",
    when(
        col("country").isNull()
        | (trim(col("country")) == ""),
        "EMPTY_COUNTRY"
    ).otherwise(None)
)


# In[94]:


silver_df.select(
    "employee_id",
    "country",
    "country_error"
).show(50, truncate=False)


# In[95]:


email_pattern = r"\A[^@\s]+@[^@\s]+\.[^@\s]+\z"


# In[96]:


silver_df = silver_df.withColumn(
    "email_error",
    when(
        col("email").isNull()
        | (trim(col("email")) == ""),
        "EMPTY_EMAIL"
    )
    .when(
        ~col("email").rlike(email_pattern),
        "INVALID_EMAIL"
    )
    .otherwise(None)
)


# In[97]:


display(silver_df)


# In[98]:


silver_df = silver_df.withColumn(
    "hire_date_error",
    when(
        col("hire_date").isNull()
        | (trim(col("hire_date")) == ""),
        "EMPTY_HIRE_DATE"
    ).when(
        col("hire_date").isNotNull()
        & col("hire_date_parsed").isNull(),
        "INVALID_HIRE_DATE"
    ).otherwise(None)
)


# In[99]:


silver_df.select(
    "hire_date_error"
).filter(
    col("hire_date_error").isNotNull()
).show(50, truncate=False)


# In[100]:


silver_df.groupBy("hire_date_error").count().show()


# In[101]:


silver_df.filter(
    col("hire_date_error").isNotNull()
).groupBy(
    "hire_date_error"
).count().show()


# ## Deduplication
# 

# In[102]:


duplicates_df = silver_df.groupBy("employee_id").count().filter(
    col("count") > 1
)

duplicates_df.show()


# In[103]:


duplicates_df.count()


# In[104]:


silver_df.printSchema()


# In[105]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

employee_window = Window.partitionBy(
    "employee_id"
).orderBy(
    col("updated_at").desc()
)


# In[106]:


silver_df = silver_df.withColumn(
    "row_num",
    row_number().over(employee_window)
)


# In[107]:


latest_df = silver_df.filter(
    col("row_num") == 1
)


# In[108]:


excluded_df = silver_df.filter(
    col("row_num") > 1
)

print(excluded_df.count())
excluded_df.show()


# In[109]:


print("Wszystkie rekordy:", silver_df.count())
print("Najnowsze rekordy:", latest_df.count())
print("Starsze duplikaty:", excluded_df.count())


# ## Error Reasons

# In[111]:


from pyspark.sql.functions import concat_ws

latest_df = latest_df.withColumn(
    "error_reasons",
    concat_ws(
        ", ",
        col("salary_error"),
        col("status_error"),
        col("country_error"),
        col("email_error"),
        col("hire_date_error")
    )
)


# In[112]:


latest_df.select(
    "employee_id",
    "salary_error",
    "status_error",
    "country_error",
    "email_error",
    "hire_date_error",
    "error_reasons"
).filter(
    col("error_reasons") != ""
).show(50, truncate=False)


# ## Split Clean and Rejected Records
# 

# In[113]:


clean_df = latest_df.filter(
    col("error_reasons") == ""
)


# In[114]:


display(clean_df)


# In[115]:


rejected_df = latest_df.filter(
    col("error_reasons") != ""
)


# In[116]:


display(rejected_df)


# ## Reconciliation Check
# 

# In[117]:


source_count = silver_df.count()
latest_count = latest_df.count()
excluded_count = excluded_df.count()
clean_count = clean_df.count()
rejected_count = rejected_df.count()

print("Source records:", source_count)
print("Latest records:", latest_count)
print("Older duplicates:", excluded_count)
print("Clean:", clean_count)
print("Rejected:", rejected_count)

print("Deduplication balance:", latest_count + excluded_count == source_count)
print("Silver balance:", clean_count + rejected_count == latest_count)
print("Final balance:", clean_count + rejected_count + excluded_count == source_count)


# ## Prepare Clean Dataset
# 

# In[118]:


clean_df = clean_df.drop(
    "row_num",
    "salary_error",
    "status_error",
    "country_error",
    "email_error",
    "hire_date_error",
    "error_reasons"
)


# In[119]:


from pyspark.sql.functions import to_date

clean_df = clean_df.withColumn(
    "hire_date",
    to_date(col("hire_date_parsed"))
)


# In[120]:


clean_df.select(
    "hire_date",
    "hire_date_parsed"
).printSchema()


# In[121]:


clean_df.printSchema()


# In[122]:


clean_df = clean_df.drop(
    "hire_date_parsed"
)


# In[123]:


clean_df.printSchema()


# In[124]:


print(clean_df.count())


# ## Write Silver Tables
# 

# In[125]:


(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dbo.employees_clean")
)


# In[126]:


display(
    spark.table("dbo.employees_clean")
)


# In[127]:


spark.table(
    "dbo.employees_clean"
).printSchema()


# In[128]:


display(rejected_df)


# In[129]:


rejected_df = rejected_df.drop(
    "row_num"
)


# In[130]:


rejected_df.printSchema()


# In[131]:


(
    rejected_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dbo.employees_rejected")
)


# In[132]:


print(
    spark.table("dbo.employees_rejected").count()
)


# In[133]:


display(excluded_df)


# In[134]:


excluded_df = excluded_df.withColumn(
    "exclusion_reason",
    lit("OLDER_DUPLICATE")
)


# In[135]:


excluded_df.select(
    "employee_id",
    "row_num",
    "exclusion_reason"
).show()


# In[136]:


(
    excluded_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dbo.employees_excluded")
)


# # Final Validation
# 

# In[137]:


clean_table_count = spark.table("dbo.employees_clean").count()
rejected_table_count = spark.table("dbo.employees_rejected").count()
excluded_table_count = spark.table("dbo.employees_excluded").count()

total_count = (
    clean_table_count
    + rejected_table_count
    + excluded_table_count
)

print("Clean:", clean_table_count)
print("Rejected:", rejected_table_count)
print("Excluded:", excluded_table_count)
print("Total:", total_count)
print("Final balance correct:", total_count == source_count)

