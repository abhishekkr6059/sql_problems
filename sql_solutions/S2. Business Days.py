# Databricks notebook source
# MAGIC %md
# MAGIC - Write a query to find business days between create date and resolved date for the tickets excluding weekends and public holidays. Order the results in increasing order of ticket_id.

# COMMAND ----------

import datetime
ticket_schema = "ticket_id int, create_date date, resolved_date date"

ticket_data  = [(1, datetime.date(2022, 8, 1), datetime.date(2022, 8, 3)),
(2, datetime.date(2022, 8, 1), datetime.date(2022, 8, 12)),
(3, datetime.date(2022, 8, 1), datetime.date(2022, 8, 16))]

ticket_df = spark.createDataFrame(ticket_data, schema=ticket_schema)
display(ticket_df)

# COMMAND ----------

ticket_df.createOrReplaceTempView("tickets")

# COMMAND ----------

import datetime
holiday_schema = "holiday_date date, reason string"
holiday_data  = [(datetime.date(2022, 8, 11),'Rakhi'), (datetime.date(2022, 8, 15),'Independence day')]

holiday_df = spark.createDataFrame(holiday_data, schema=holiday_schema)
display(holiday_df)

# COMMAND ----------

holiday_df.createOrReplaceTempView("holidays")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tickets;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM holidays;

# COMMAND ----------

# MAGIC %md
# MAGIC - Sample Output
# MAGIC |ticket_id|create_date|resolved_date|no_of_holidays|actual_days|business_days|
# MAGIC |---------|-----------|-------------|--------------|-----------|-------------|
# MAGIC |1|2022-08-01|2022-08-03|0|2|2|
# MAGIC |2|2022-08-01|2022-08-12|1|11|8|
# MAGIC |3|2022-08-01|2022-08-16|2|15|9|

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC datediff(day, create_date, resolved_date) AS actual_days,
# MAGIC datediff(day, create_date, resolved_date) - 2 * datediff(week, create_date, resolved_date) - no_of_holidays AS business_days
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT ticket_id, create_date, resolved_date, count(holiday_date) AS no_of_holidays
# MAGIC   FROM tickets
# MAGIC   LEFT JOIN holidays
# MAGIC   ON holiday_date BETWEEN create_date AND resolved_date AND weekday(holiday_date) NOT IN (5,6)
# MAGIC   GROUP BY ticket_id, create_date, resolved_date
# MAGIC )
# MAGIC ORDER BY ticket_id
