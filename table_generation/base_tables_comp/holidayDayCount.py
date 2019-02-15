# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

holidayCohortTable = spark.read.format("delta").load(dbutils.widgets.get("holidayCohortTable_table_path"))
holidayDayCountPath = dbutils.widgets.get("holidayDayCount_table_path")

# holidayCohortTable = spark.table('insight.outdoors_retail_holidayCohortTable')
# holidayDayCountPath = 'insight.outdoors_retail_holidayDayCount'

# COMMAND ----------

# Find the number of days in each holiday
holidayDayCount = (holidayCohortTable
                    .groupBy('holiday', 'years')
                    .agg(F.countDistinct('date').alias('days'))
                   )

# COMMAND ----------

# holidayDayCount.write.saveAsTable(holidayDayCountPath, mode = 'overwrite')

# COMMAND ----------

holidayDayCount.write.mode('overwrite').format('delta').save(holidayDayCountPath)

# COMMAND ----------

