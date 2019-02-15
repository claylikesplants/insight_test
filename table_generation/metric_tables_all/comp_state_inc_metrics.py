# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

dayNum = spark.read.format("delta").load(dbutils.widgets.get("dayNum_table_path"))
baseUsersCount = spark.read.format("delta").load(dbutils.widgets.get("baseUsersCount_table_path"))
baseTable = spark.table(dbutils.widgets.get("baseTable_table_path"))
baseGeoIncomeTagging = spark.read.format("delta").load(dbutils.widgets.get("baseGeoIncomeTagging_table_path"))
income_order = spark.read.format("delta").load(dbutils.widgets.get("income_order_table_path"))
comp_state_inc_metrics_path = dbutils.widgets.get("comp_state_inc_metrics_table_path")

# dayNum =  spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/dayNum')
# baseUsersCount = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/baseUsersCount')
# baseTable = spark.table('insight.baseTable')
# baseGeoIncomeTagging = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/baseGeoIncomeTagging')
# income_order = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/income_order')
# comp_state_inc_metrics_path = 'dbfs:/mnt/mscience-dev/databases/insight/comp_state_inc_metrics'

# COMMAND ----------

def gen_comp_state_inc_metrics(baseTable, dayNum, baseUsersCount, baseGeoIncomeTagging, income_order, time_period):
  """Generate a table showing spend and transactions by company in a given time period, segmented by income and state"""
  dayNum = dayNum.where(F.col('time_period') == time_period).drop('time_period')
  baseUsersCount = baseUsersCount.where(F.col('time_period') == time_period).drop('time_period')
  baseGeoIncomeTagging = baseGeoIncomeTagging.where(F.col('time_period') == time_period).drop('time_period')
  comp_state_inc_metrics_df = (baseTable
                               .withColumn('date', F.date_trunc(time_period, 'date'))
                               .join(baseGeoIncomeTagging, ['date', 'unique_mem_id'], 'left')
                               #Aggregate over time period
                               .groupBy('date', 'state', 'census_cat', 'company', 'vertical', 'sub_vertical', 'region')
                               .agg(F.sum('transactions').alias('transactions'),
                                     F.sum('amount').alias('amount')
                                   )
                               .withColumn('amount', F.col('amount').cast(DoubleType()))
                               .withColumn('transactions', F.col('transactions').cast(DoubleType()))
                               .join(baseUsersCount, ['date'])
                               .join(dayNum, ['date'])
                               .join(income_order, ['census_cat'])
                               .withColumn('transactions', (F.col('transactions') / F.col('unique_users')))
                              .withColumn('amount', (F.col('amount') / F.col('unique_users')))
                               .withColumn('time_period', F.lit(time_period))
                               .withColumn('time_period_num', F.when(F.col('time_period') == 'quarter', ((F.month('date') - 1) / 3 + 1).cast('int').cast('string'))
                                           .when(F.col('time_period') == 'month', F.month('date').cast('string'))
                                           .when(F.col('time_period') == 'year', F.lit(''))
                                           .otherwise(F.lit(''))
                                          )
                               .withColumn('year', F.when(F.col('time_period') == 'year', F.lit(''))
                                           .otherwise(F.year('date').cast('string'))
                                          )
                               .withColumn('date_category', F.concat_ws(' ', 'year', 'time_period', 'time_period_num'))
                               .withColumn('date_category', F.rtrim(F.col('date_category')))
                               .drop('unique_users', 'days', 'internal', 'time_period_num', 'year')            
                              )
  return comp_state_inc_metrics_df

# COMMAND ----------

time_periods = ['month', 'year', 'quarter']
inputs = [baseTable, dayNum, baseUsersCount, baseGeoIncomeTagging, income_order]
time_period_dfs = gen_time_period_dfs(gen_comp_state_inc_metrics, inputs, time_periods)
comp_state_inc_metrics = combine_time_periods(time_period_dfs)

# COMMAND ----------

comp_state_inc_metrics.write.mode('overwrite').format('delta').save(comp_state_inc_metrics_path)

# COMMAND ----------

