# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

baseUsersCount = spark.read.format("delta").load(dbutils.widgets.get("baseUsersCount_table_path"))
cohortTable = spark.read.format("delta").load(dbutils.widgets.get("cohortTable_table_path"))
retention_path = dbutils.widgets.get("retention_table_path")

# baseUsersCount = spark.table('insight.baseUsersCount')
# cohortTable = spark.table('insight.backcountry_cohorttable')
# retention_path = 'insight.backcountry_retention'

# COMMAND ----------

def sub_dates(date1, date2, time_period):
  """Take the difference between two dates using a given time measurement"""
  month_diff = date1.month - date2.month
  year_diff = date1.year - date2.year
  if time_period == 'year':
    return year_diff
  month_diff_total = month_diff + year_diff * 12
  if time_period == 'month':
    return month_diff_total
  if time_period == 'quarter':
    d1_quarter = math.ceil(date1.month / 3.)
    d2_quarter = math.ceil(date2.month / 3.)
    quarter_diff = d1_quarter - d2_quarter
    return int(quarter_diff + year_diff * 4)
  
udf_sub_dates = F.udf(sub_dates, IntegerType())

# COMMAND ----------

def num_to_date(num):
  return datetime.date(2000 + num, 1, 1)

udf_num_to_date = F.udf(num_to_date, DateType())

# COMMAND ----------

def gen_retention(baseUsersCount, cohortTable, time_period, method):
  """Generate a table showing user retention based on a time period and counting method

  For all dates in baseUsersCount, of the people who transacted
  with a company in a given time period, how many of those same
  users transact with the same company in subsequent time periods?
  The two methods are drop_nl_users and keep_nl_users, where the
  first method drops users who have gaps in their purchases with
  a company and the second doesn't.
  """
  baseUsersCount = (baseUsersCount
                    .where(F.col('time_period') == time_period)
                    .drop('time_period')
                    .orderBy(F.col('date'))
                    .withColumnRenamed('date', 'date_base')
                   )
  # Aggregate over given time period
  cohortTableTimePeriod = (cohortTable
                          .withColumn('date', F.date_trunc(time_period, 'date'))
                          .groupBy('unique_mem_id', 'date', 'clean_company')
                          .agg(F.sum('transactions').alias('transactions'))
                          )
  base_table = (cohortTableTimePeriod
              .withColumnRenamed('date', 'date_base')
              .drop('transactions')
             )
  # Join all aggregated transactions by user, company, time period with itself
  retention_df = (base_table
                  .join(cohortTableTimePeriod, ['unique_mem_id', 'clean_company'], 'inner')
                  # Make sure to select only sequential pairs of transactions
                  .where((F.col('date_base') <= F.col('date')))
                  .withColumn('time_period', F.lit(time_period))
                  .withColumn('period_elapsed', udf_sub_dates(F.col('date'), F.col('date_base'), F.col('time_period')))

                  )
  if method == 'drop_nl_users':
    # Find the first time after
    W = Window.partitionBy('unique_mem_id', 'clean_company', 'date_base').orderBy(F.asc('period_elapsed'))
    trans_break = (retention_df
                   .withColumn('prev_period_elapsed', F.lag('period_elapsed').over(W))
                   .where(~F.col('prev_period_elapsed').isNull())
                  .withColumn('gap', F.when((F.col('period_elapsed') - F.col('prev_period_elapsed')) > 1, True)
                              .otherwise(False)
                             )
                 .where(F.col('gap'))
                 .groupBy('unique_mem_id', 'clean_company', 'date_base')
                 .agg(F.min('period_elapsed').alias('first_break_period'))
                 )
    retention_df = (retention_df
                    .join(trans_break, ['unique_mem_id', 'clean_company', 'date_base'], 'left')
                    .where((F.col('period_elapsed') < F.col('first_break_period')) |
                           F.isnull("first_break_period")
                          )
                   )

    retention_df_uu = (retention_df
                       .groupBy('date_base', 'clean_company')
                       .agg(F.countDistinct('unique_mem_id').alias('unique_users_base'))
                      )

  else:

    retention_df_uu = (retention_df
                       .groupBy('date_base', 'clean_company')
                       .agg(F.countDistinct('unique_mem_id').alias('unique_users_base'))
                      )

  retention_df = (retention_df
                  .join(retention_df_uu, ['date_base', 'clean_company'])
                  .groupBy('clean_company', 'unique_users_base', 'date', 'date_base', 'period_elapsed', 'time_period')
                  .agg(F.countDistinct('unique_mem_id').alias('retained_users'))
                  .join(baseUsersCount, ['date_base'])
                  .withColumn('period_elapsed_date', udf_num_to_date(F.col('period_elapsed')))
                    .withColumn('retained_users', F.col('retained_users') / F.col('unique_users'))
                    .withColumn('unique_users_base', F.col('unique_users_base') / F.col('unique_users'))
                  .withColumn('date_base_cat', F.date_format('date_base', 'MM/dd/yyyy'))
                  .withColumn('method', F.lit(method))
                  .drop('unique_users')
                   )
  
  return retention_df

# COMMAND ----------

time_periods = ['month', 'year', 'quarter']
methods = ['drop_nl_users', 'keep_nl_users']
inputs = [baseUsersCount, cohortTable]
time_period_dfs = gen_time_period_dfs_r(gen_retention, inputs, time_periods, methods)
retention = combine_time_periods(time_period_dfs)

# COMMAND ----------

retention.write.mode('overwrite').format('delta').save(retention_path)

# COMMAND ----------

aurora_insight_upload(retention_path)

# COMMAND ----------

