# Databricks notebook source
# MAGIC %md
# MAGIC - Write a query to return all employees who salary is same in the same department

# COMMAND ----------

import datetime
schema = "emp_id int, name string, salary string, dept_id string"

data  = [(101, 'sohan', '3000', '11'),
(102, 'rohan', '4000', '12'),
(103, 'mohan', '5000', '13'),
(104, 'cat', '3000', '11'),
(105, 'suresh', '4000', '12'),
(109, 'mahesh', '7000', '12'),
(108, 'kamal', '8000', '11')]

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("emp_salary")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM emp_salary;

# COMMAND ----------

# MAGIC %md
# MAGIC - Sample Output
# MAGIC |emp_id|name|salary|dept_id|
# MAGIC |------|----|------|-------|
# MAGIC |101|sohan|3000|11|
# MAGIC |102|rohan|4000|12|
# MAGIC |104|cat|3000|11|
# MAGIC |105|suresh|4000|12|

# COMMAND ----------


