#!/usr/bin/env python
# coding: utf-8

# ## gold_reporting_fabric
# 
# New notebook

# # Gold Reporting
# 
# This notebook transforms validated Silver employee data into business-ready Gold datasets for reporting and analytics.
# 
# Source:
# `people_data_silver.dbo.employees_clean`
# 
# Target:
# `people_data_gold`

# In[1]:


silver_df = spark.table(
    "people_data_silver.dbo.employees_clean"
)

display(silver_df)


# In[2]:


silver_df.count()


# ## Summary Tables

# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# CREATE OR REPLACE TABLE dbo.department_summary AS
# SELECT
#     department_id,
#     COUNT(*) AS employee_count,
#     ROUND(AVG(salary), 2) AS average_salary
# FROM people_data_silver.dbo.employees_clean
# GROUP BY department_id


# In[4]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# CREATE OR REPLACE TABLE dbo.country_summary AS
# SELECT
#     country,
#     COUNT(*) AS employee_count,
#     ROUND(AVG(salary), 2) AS average_salary
# FROM people_data_silver.dbo.employees_clean
# GROUP BY country


# In[5]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.country_summary


# In[7]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# CREATE OR REPLACE TABLE dbo.employment_type_summary AS
# SELECT
#     employment_type,
#     COUNT(*) AS employee_count,
#     ROUND(AVG(salary), 2) AS average_salary
# FROM people_data_silver.dbo.employees_clean
# GROUP BY employment_type


# In[8]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.employment_type_summary


# In[9]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# CREATE OR REPLACE TABLE dbo.status_summary AS
# SELECT
#     status,
#     COUNT(*) AS employee_count,
#     ROUND(
#         COUNT(*) * 100.0 /
#         (SELECT COUNT(*) FROM people_data_silver.dbo.employees_clean),
#         2
#     ) AS percentage
# FROM people_data_silver.dbo.employees_clean
# GROUP BY status


# In[10]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.status_summary


# In[11]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# CREATE OR REPLACE TABLE dbo.hiring_summary AS
# SELECT
#     YEAR(hire_date) AS hire_year,
#     COUNT(*) AS employee_count
# FROM people_data_silver.dbo.employees_clean
# GROUP BY YEAR(hire_date)


# In[12]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.hiring_summary
# ORDER BY employee_count DESC


# ## Check Results

# In[13]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.department_summary


# In[14]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.country_summary


# In[15]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.employment_type_summary


# In[16]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.status_summary


# In[17]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# SELECT *
# FROM dbo.hiring_summary
# ORDER BY employee_count DESC

