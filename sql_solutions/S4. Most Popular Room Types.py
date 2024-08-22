# Databricks notebook source
# MAGIC %md
# MAGIC - Write a query to find the room types that are searched most number of times
# MAGIC - Output the room type along with the number of searches for it
# MAGIC - If the filter for room type has more than one room type, consider each unique room as a separate row
# MAGIC - Sort the result based on the number of searches in descending order & filter room type value in ascending order

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

# MAGIC %sql
# MAGIC WITH cte AS 
# MAGIC (
# MAGIC   SELECT explode(split(filter_room_types, ",")) AS room_type FROM airbnb_searches
# MAGIC )
# MAGIC SELECT room_type, COUNT(room_type) AS no_of_searches
# MAGIC FROM cte
# MAGIC GROUP BY room_type
# MAGIC ORDER BY COUNT(room_type) DESC, room_type ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- sql server solution
# MAGIC -- SELECT value AS room_type, COUNT(1) AS no_of_searches FROM airbnb_searches
# MAGIC -- CROSS APPLY STRING_SPLIT(filter_room_types, ",")
# MAGIC -- GROUP BY value
# MAGIC -- ORDER BY no_of_searches DESC, room_type ASC
