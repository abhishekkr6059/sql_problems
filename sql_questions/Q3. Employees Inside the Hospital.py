# Databricks notebook source
# MAGIC %md
# MAGIC - Write a query to get the list of employees present inside the hospital

# COMMAND ----------

import datetime
schema = "emp_id int, action string, time timestamp"

data  = [(1, 'in', datetime.datetime(2019, 12, 22, 9, 0, 0)),
 (1, 'out', datetime.datetime(2019, 12, 22, 9, 15, 0)),
 (2, 'in', datetime.datetime(2019, 12, 22, 9, 0, 0)),
 (2, 'out', datetime.datetime(2019, 12, 22, 9, 15, 0)),
 (2, 'in', datetime.datetime(2019, 12, 22, 9, 30, 0)),
 (3, 'out', datetime.datetime(2019, 12, 22, 9, 0, 0)),
 (3, 'in', datetime.datetime(2019, 12, 22, 9, 15, 0)),
 (3, 'out', datetime.datetime(2019, 12, 22, 9, 30, 0)),
 (3, 'in', datetime.datetime(2019, 12, 22, 9, 45, 0)),
 (4, 'in', datetime.datetime(2019, 12, 22, 9, 45, 0)),
 (5, 'out', datetime.datetime(2019, 12, 22, 9, 40, 0))]

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("hospital")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hospital;

# COMMAND ----------

# MAGIC %md
# MAGIC - sample output
# MAGIC |emp_id|
# MAGIC |------|
# MAGIC |2|
# MAGIC |3|
# MAGIC |4|

# COMMAND ----------


