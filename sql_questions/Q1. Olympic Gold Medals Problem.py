# Databricks notebook source
# MAGIC %md
# MAGIC - Write a query to find number of gold medals per swimmer, for swimmers who won only gold medals, order by number of medals in descending order

# COMMAND ----------

schema = "ID int, event string, YEAR int, GOLD string, SILVER string, BRONZE string"

data  = [[1,'100m',2016, 'Amthhew Mcgarray','donald','barbara'],
[2,'200m',2016, 'Nichole','Alvaro Eaton','janet Smith'],
[3,'500m',2016, 'Charles','Nichole','Susana'],
[4,'100m',2016, 'Ronald','maria','paula'],
[5,'200m',2016, 'Alfred','carol','Steven'],
[6,'500m',2016, 'Nichole','Alfred','Brandon'],
[7,'100m',2016, 'Charles','Dennis','Susana'],
[8,'200m',2016, 'Thomas','Dawn','catherine'],
[9,'500m',2016, 'Thomas','Dennis','paula'],
[10,'100m',2016, 'Charles','Dennis','Susana'],
[11,'200m',2016, 'jessica','Donald','Stefeney'],
[12,'500m',2016,'Thomas','Steven','Catherine']]

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("events")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events;

# COMMAND ----------

# MAGIC %md
# MAGIC - sample output
# MAGIC |player_name|medal_count|
# MAGIC |-----------|-----------|
# MAGIC |Charles|3|
# MAGIC |Thomas|3|
# MAGIC |Amthhew Mcgarray|1|
# MAGIC |Ronald|1|
# MAGIC |jessica|1|

# COMMAND ----------


