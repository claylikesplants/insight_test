# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

baseTable = spark.table(dbutils.widgets.get("baseTable_table_path"))
allClientHolidays = spark.read.format("delta").load(dbutils.widgets.get("allClientHolidays_table_path"))
holidayUsersCountPath = dbutils.widgets.get("holidayUsersCount_table_path")
clientName = dbutils.widgets.get("clientName")

# baseTable = spark.table('insight.baseTable')
# allClientHolidays = spark.table('insight.allClientHolidays')
# holidayUsersCountPath = 'insight.shutterfly_holidayUsersCount'
# clientName = 'Shutterfly'

# COMMAND ----------

clientHolidays = allClientHolidays.where(F.col('client') == clientName)

# COMMAND ----------

# Find number of users for each holiday period
holidayUsersCount = (baseTable
                     .join(clientHolidays, ['date'])
                      .groupBy('holiday', 'years')
                      .agg(F.countDistinct('unique_mem_id').alias('unique_users'))
                     )

# COMMAND ----------

holidayUsersCount.write.mode('overwrite').format('delta').save(holidayUsersCountPath)