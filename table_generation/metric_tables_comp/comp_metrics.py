# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

cohortTable = spark.read.format("delta").load(dbutils.widgets.get("cohortTable_table_path"))
baseUsersCount = spark.read.format("delta").load(dbutils.widgets.get("baseUsersCount_table_path"))
dayNum = spark.read.format("delta").load(dbutils.widgets.get("dayNum_table_path"))
txnTimeSpan = spark.read.format("delta").load(dbutils.widgets.get("txnTimeSpan_table_path"))
comp_metrics_path = dbutils.widgets.get("comp_metrics_table_path")

# cohortTable = spark.read.format("delta").load("dbfs:/mnt/mscience-dev/databases/insight/basetable")
# baseUsersCount = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/baseuserscount')
# dayNum = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/daynum')
# txnTimeSpan = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/grocery_outlet_txntimespan')
# comp_metrics_path = 'insight.grocery_outlet_comp_metrics'

# COMMAND ----------

def gen_comp_metrics(cohortTable, baseUsersCount, dayNum, txnTimeSpan, time_period = 'month'):
  """Generate a table showing transactions per user, spend per user, average transaction, and time between orders in a given period of time"""
  baseUsersCount = baseUsersCount.where(F.col('time_period') == time_period).drop('time_period')
  dayNum = dayNum.where(F.col('time_period') == time_period).drop('time_period')
  txnTimeSpan = txnTimeSpan.where(F.col('time_period') == time_period).drop('time_period')
  comp_metrics_df = (cohortTable
                     .withColumn('date', F.date_trunc(time_period, 'date'))
                     .groupBy('date', 'clean_company', 'insightVertical', 'insightSubvertical')
                     .agg(F.sum('transactions').alias('transactions'),
                           F.sum('amount').alias('amount'), 
                          F.countDistinct('unique_mem_id').alias('unique_users')
                          )
                     .join(dayNum, ['date'])
                    .withColumn('amount', F.col('amount').cast(DoubleType()))
                     .withColumn('transactions', F.col('transactions').cast(DoubleType()))
                     .withColumn('transactions_per_user', (F.col('transactions') / F.col('unique_users')))
                     .withColumn('amount_per_user', (F.col('amount') / F.col('unique_users')))
                     .withColumn('average_transaction', F.col('amount') / F.col('transactions'))
                     .join(txnTimeSpan, ['date', 'clean_company'])
                     .drop('unique_users', 'days', 'transactions', 'amount')
                     .withColumn('time_period', F.lit(time_period))
                    )
  return comp_metrics_df

# COMMAND ----------

time_periods = ['month', 'year', 'quarter']
inputs = [cohortTable, baseUsersCount, dayNum, txnTimeSpan]
time_period_dfs = gen_time_period_dfs(gen_comp_metrics, inputs, time_periods)
comp_metrics = combine_time_periods(time_period_dfs)

# COMMAND ----------

comp_metrics.write.mode('overwrite').format('delta').save(comp_metrics_path)

# COMMAND ----------

aurora_insight_upload(comp_metrics_path)

# COMMAND ----------

