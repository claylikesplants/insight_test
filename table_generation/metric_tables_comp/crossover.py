# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

baseTable = spark.table(dbutils.widgets.get("baseTable_table_path"))
baseUsersCount = spark.read.format("delta").load(dbutils.widgets.get("baseUsersCount_table_path"))
cohortTable = spark.read.format("delta").load(dbutils.widgets.get("cohortTable_table_path"))
crossover_path = dbutils.widgets.get("crossover_table_path")

# baseTable = spark.table('insight.basetable')
# baseUsersCount = spark.table('insight.baseUsersCount')
# cohortTable = spark.table('insight.backcountry_cohorttable')
# crossover_path = 'insight.backcountry_crossover'

# COMMAND ----------

def gen_crossover(baseTable, baseUsersCount, cohortTable, time_period = 'month'): 
  """Generate a table that shows where a users spent, given they transacted at a given company in a given time period"""
  baseUsersCount = baseUsersCount.where(F.col('time_period') == time_period).drop('time_period')
  # Aggregate over time period for cohortTable
  baseTableMetrics = (cohortTable
                      .withColumn('date', F.date_trunc(time_period, 'date'))
                      .groupBy('unique_mem_id', 'clean_company', 'date')
                      .agg(
                            F.sum('transactions').alias('transactions')
                           )
                      .withColumnRenamed('clean_company', 'clean_companyRef')
                      .withColumnRenamed('transactions', 'transactionsRef')
                     )
  # Aggregate over time period for baseTable
  reffBaseTableMetrics = (baseTable
                        .withColumn('date', F.date_trunc(time_period, 'date'))
                        .groupBy('unique_mem_id', 'clean_company', 'date', 'InsightVertical', 'InsightSubvertical')
                        .agg(
                              F.sum('transactions').alias('transactions'),
                              F.sum('amount').alias('amount')
                             )
                         )
  # Join aggregated transactions in both tables
  crossover_all = (reffBaseTableMetrics
                    .join(baseTableMetrics, on = ['date', 'unique_mem_id'], how = 'inner')
                  )
  W = Window.orderBy("date").partitionBy('clean_companyRef')
  compPeriodMetrics = (crossover_all
                       .groupBy('date', 'clean_companyRef')
                       .agg(
                         F.sum('transactions').alias('total_transactions'),
                         F.sum('amount').alias('total_amount')
                         )
                      )
  crossover_all = (crossover_all
                    .groupBy('date', 'clean_companyRef', 'clean_company', 'InsightVertical', 'InsightSubvertical', 'transactionsRef') 
                   .agg(
                         F.sum('transactions').alias('transactions'),
                         F.sum('amount').alias('amount')
                         )
                   .join(compPeriodMetrics, ['date', 'clean_companyRef'])
                   .withColumn('transactions', F.col('transactions').cast(DoubleType()))
                   .withColumn('amount', F.col('amount').cast(DoubleType()))
                   .withColumn('transactions', F.col('transactions') / F.col('total_transactions'))
                   .withColumn('amount', F.col('amount') / F.col('total_amount'))
                   .withColumn('time_period', F.lit(time_period))
                   .drop('total_transactions', 'total_amount')
                    )
  
  return crossover_all

# COMMAND ----------

time_periods = ['month', 'year', 'quarter']
inputs = [baseTable, baseUsersCount, cohortTable]
time_period_dfs = gen_time_period_dfs(gen_crossover, inputs, time_periods)
crossover = combine_time_periods(time_period_dfs)

# COMMAND ----------

crossover.write.mode('overwrite').format('delta').save(crossover_path)

# COMMAND ----------

aurora_insight_upload(crossover_path)

# COMMAND ----------

