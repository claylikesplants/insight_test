# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

cohortTable = spark.read.format("delta").load(dbutils.widgets.get("cohortTable_table_path"))
txnTimeSpanPath = dbutils.widgets.get("txnTimeSpan_table_path")

# cohortTable = spark.table('insight.basetable')
# txnTimeSpanPath = 'dbfs:/mnt/mscience-dev/databases/insight/txnTimeSpan'

# COMMAND ----------

# Order transactions by date over a user and company and give each transaction a rank
cohortTable_daily_ordered = (cohortTable
                              .withColumn
                           ('order_index',F.rank().over(Window.partitionBy(F.col('unique_mem_id'), F.col('clean_company')).orderBy(F.desc('date'))))
                             )

# COMMAND ----------

cohortTable_daily_ordered_base = (cohortTable_daily_ordered
                                   .withColumnRenamed('order_index', 'order_index_base')
                                   .withColumnRenamed('date', 'date_base')
                                  )

# COMMAND ----------

time_between_orders = (cohortTable_daily_ordered_base
                        .join(cohortTable_daily_ordered, ['unique_mem_id', 'clean_company'], 'inner')
                       # Pair up chronological transactions
                        .where('order_index_base = (order_index - 1)')
                        .withColumn('days_from_last_order', F.datediff('date_base','date'))
                       )

# COMMAND ----------

days_list = (cohortTable
             .select(F.col('date').alias('day'))
             .distinct()
            )

# COMMAND ----------

def get_time_between_orders(time_between_orders, time_period):
  time_between_orders_agg = (time_between_orders
                            .withColumn('date', F.date_trunc(time_period, F.col('date')))
                            .groupBy('date', 'clean_company')
                            .agg(F.avg('days_between_orders').alias('days_between_orders'))
                            .withColumn('time_period', F.lit(time_period))
                           )
  return time_between_orders_agg

# COMMAND ----------

def rolling_28_day_time_between_orders(time_between_orders):
  """Find average days between orders for a given time period
  
  For each day in a given period at a given company, look at
  all pairs of transactions where the second transaction is
  in the previous 28 days. Average the time between those
  transactions together for each of the days, then average
  the days together.
  """
  time_between_orders_28 = (time_between_orders
                              .where('!(date_base = date)')
                             .where("days_from_last_order <= 200")
                            # Find the transaction pairs in the last 28 days
                              .join(days_list, [F.col('date_base') <= F.col('day'), F.date_add(F.col('date_base'), 27) >= F.col('day')])
                              .groupBy('day', 'clean_company')
                              .agg(F.avg('days_from_last_order').alias('days_between_orders'))
                              .withColumnRenamed('day', 'date')
                             )
  return time_between_orders_28

# COMMAND ----------

def time_between_orders_time_period(time_between_orders_28, time_period):
  """Find average days between orders for a given time period"""
  time_between_orders_time_period_df = (time_between_orders_28
                                        .withColumn('date', F.date_trunc(time_period, F.col('date')))
                                        .groupBy('date', 'clean_company')
                                        .agg(F.avg('days_between_orders').alias('days_between_orders'))
                                        .withColumn('time_period', F.lit(time_period))
                                       )
  return time_between_orders_time_period_df

# COMMAND ----------

time_between_orders_28 = rolling_28_day_time_between_orders(time_between_orders)

# COMMAND ----------

time_periods = ['month', 'year', 'quarter']
inputs = [time_between_orders_28]
time_period_dfs = gen_time_period_dfs(get_time_between_orders, inputs, time_periods)
txnTimeSpan = combine_time_periods(time_period_dfs)

# COMMAND ----------

txnTimeSpan.write.mode('overwrite').format('delta').save(txnTimeSpanPath)

# COMMAND ----------

