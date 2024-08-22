# Databricks notebook source
# MAGIC %md
# MAGIC - Write a query to find the room types that are searched most number of times
# MAGIC - Output the room type along with the number of searches for it
# MAGIC - If the filter for room type has more than one room type, consider each unique room as a separate row
# MAGIC - Sort the result based on the number of searches in descending order

# COMMAND ----------

import datetime
schema = "user_id int, date_searched date, filter_room_types string"

data  = [(1, datetime.date(2022, 1, 1),'entire home,private room'),
         (2, datetime.date(2022, 1, 2),'entire home,shared room'),
         (3, datetime.date(2022, 1, 2),'private room,shared room'),
         (4, datetime.date(2022, 1, 3),'private room')]

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("airbnb_searches")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM airbnb_searches;

# COMMAND ----------

# MAGIC %md
# MAGIC - Sample Output
# MAGIC |room_type|no_of_searches|
# MAGIC |-----|-----|
# MAGIC |private room|3|
# MAGIC |entire room|2|
# MAGIC |shared room|2|

# COMMAND ----------


