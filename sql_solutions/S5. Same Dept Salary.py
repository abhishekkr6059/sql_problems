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

# MAGIC %sql
# MAGIC -- method 1
# MAGIC SELECT e1.emp_id, e1.name, e1.salary, e1.dept_id
# MAGIC FROM emp_salary e1
# MAGIC INNER JOIN emp_salary e2
# MAGIC ON e1.salary = e2.salary AND e1.dept_id = e2.dept_id AND e1.emp_id != e2.emp_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- method 2
# MAGIC WITH dept_sal AS 
# MAGIC (
# MAGIC   SELECT dept_id, salary 
# MAGIC   FROM emp_salary
# MAGIC   GROUP BY dept_id, salary
# MAGIC   HAVING COUNT(1) > 1
# MAGIC )
# MAGIC SELECT es.* FROM emp_salary es
# MAGIC INNER JOIN dept_sal ds
# MAGIC ON es.dept_id = ds.dept_id AND es.salary = ds.salary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- method 3
# MAGIC WITH dept_sal AS 
# MAGIC (
# MAGIC   SELECT dept_id, salary FROM emp_salary
# MAGIC   GROUP BY dept_id, salary
# MAGIC   HAVING COUNT(1) = 1
# MAGIC )
# MAGIC SELECT es.* FROM emp_salary es
# MAGIC LEFT JOIN dept_sal ds
# MAGIC ON es.dept_id = ds.dept_id AND es.salary = ds.salary
# MAGIC WHERE ds.dept_id IS NULL
