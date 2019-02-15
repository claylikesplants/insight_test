# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

baseTable = spark.table(dbutils.widgets.get("baseTable_table_path"))
baseUsersCountPath = dbutils.widgets.get("baseUsersCount_table_path")

# baseTable = spark.table('insight.baseTable')
# baseUsersCountPath = 'insight.baseUsersCount'

# COMMAND ----------

def gen_baseUsersCount(baseTable, time_period):
  """Return a dataframe containing unique users in all time periods for the baseTable"""
  if time_period != 'day':
    baseTable = (baseTable
                    .withColumn('date', F.date_trunc(time_period, F.col('date')))
                    .withColumn('date', F.trunc(F.col('date'), 'month'))
                   )
  baseUsersCount_time_period = (baseTable
                                .groupBy('date')
                                .agg(F.countDistinct('unique_mem_id').alias('unique_users'))
                                .withColumn('time_period', F.lit(time_period))
                               )
  return baseUsersCount_time_period

# COMMAND ----------

time_periods = ['day', 'month', 'year', 'quarter']
inputs = [baseTable]
time_period_dfs = gen_time_period_dfs(gen_baseUsersCount, inputs, time_periods)
baseUsersCount = combine_time_periods(time_period_dfs)

# COMMAND ----------

baseUsersCount.write.mode('overwrite').format('delta').save(baseUsersCountPath)

# COMMAND ----------

