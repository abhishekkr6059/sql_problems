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

# MAGIC %sql
# MAGIC -- method 1
# MAGIC WITH in_time AS 
# MAGIC (
# MAGIC   SELECT emp_id, MAX(time) AS latest_in_time
# MAGIC   FROM hospital
# MAGIC   WHERE action="in"
# MAGIC   GROUP BY emp_id
# MAGIC ),
# MAGIC out_time AS 
# MAGIC (
# MAGIC   SELECT emp_id, MAX(time) AS latest_out_time
# MAGIC   FROM hospital
# MAGIC   WHERE action="out"
# MAGIC   GROUP BY emp_id
# MAGIC )
# MAGIC SELECT * 
# MAGIC FROM in_time
# MAGIC LEFT JOIN out_time
# MAGIC ON in_time.emp_id = out_time.emp_id
# MAGIC WHERE latest_in_time > latest_out_time OR latest_out_time IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- method 2
# MAGIC WITH latest_time AS 
# MAGIC (
# MAGIC   SELECT emp_id, MAX(time) AS max_latest_time FROM hospital GROUP BY emp_id
# MAGIC ),
# MAGIC latest_in_time AS 
# MAGIC (
# MAGIC   SELECT emp_id, MAX(time) AS max_in_time FROM hospital WHERE action="in" GROUP BY emp_id 
# MAGIC )
# MAGIC SELECT * FROM latest_time lt 
# MAGIC INNER JOIN latest_in_time lit
# MAGIC ON lt.max_latest_time = lit.max_in_time AND lt.emp_id = lit.emp_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- method 3
# MAGIC WITH cte AS 
# MAGIC (
# MAGIC   SELECT *, rank() OVER (PARTITION BY emp_id ORDER BY time DESC) AS rnk FROM hospital
# MAGIC )
# MAGIC SELECT emp_id, action, time 
# MAGIC FROM cte WHERE rnk=1 AND action="in"
# MAGIC
