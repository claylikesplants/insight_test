# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

allClientHolidays = spark.read.format("delta").load(dbutils.widgets.get("allClientHolidays_table_path"))
cohortTable = spark.read.format("delta").load(dbutils.widgets.get("cohortTable_table_path"))
clientName = dbutils.widgets.get("clientName")
holidayCohortTablePath = dbutils.widgets.get("holidayCohortTable_table_path")

# allClientHolidays = spark.table('insight.allClientHolidays')
# cohortTable = spark.table('insight.outdoors_retail_cohortTable')
# clientName = 'Outdoors Retail'
# holidayCohortTablePath = 'insight.outdoors_retail_holidaycohorttable'

# COMMAND ----------

# Select all days in holidays associated with client
clientHolidays = allClientHolidays.where(F.col('client') == clientName)

# COMMAND ----------

holidayCohortTable = cohortTable.join(allClientHolidays, ['date'])

# COMMAND ----------

holidayCohortTable.write.mode('overwrite').format('delta').save(holidayCohortTablePath)

# COMMAND ----------

