# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

cohortTable = spark.read.format("delta").load(dbutils.widgets.get("cohortTable_table_path"))
baseUsersCount = spark.read.format("delta").load(dbutils.widgets.get("baseUsersCount_table_path"))
baseGeoIncomeTagging = spark.read.format("delta").load(dbutils.widgets.get("baseGeoIncomeTagging_table_path"))
user_breakdown_path = dbutils.widgets.get("user_breakdown_table_path")

# cohortTable = spark.table('insight.backcountry_cohorttable')
# baseUsersCount = spark.table('insight.baseUsersCount')
# baseGeoIncomeTagging = spark.table('insight.baseGeoIncomeTagging')
# user_breakdown_path = 'insight.backcountry_user_breakdown'

# COMMAND ----------

def datetime_to_date(date):
  """Converts datetime.datetime object to datetime.date object"""
  return date.date()

udf_datetime_to_date = F.udf(datetime_to_date, DateType())

# COMMAND ----------

def gen_user_breakdown(cohortTable, baseUsersCount, baseGeoIncomeTagging, time_period):
  """Generates a dataframe categorizing users at companies in a given time period by when they first transacted
  
  Users who have not yet transacted at the company are categorized as new users. Dataframe also
  divides spend and transactions into the same categories.
  """
  mainTable  = cohortTable\
                 .withColumn('date', F.date_trunc(time_period, F.col('date')))\
                 .withColumn('date', udf_datetime_to_date(F.col('date')))
                 
  
  baseUsersCount = baseUsersCount.where(F.col('time_period') == time_period).drop('time_period').withColumnRenamed('unique_users', 'unique_users_date')
  baseGeoIncomeTagging = baseGeoIncomeTagging.where(F.col('time_period') == time_period).drop('time_period')

  first_trans = (mainTable
                 .groupBy('unique_mem_id', 'clean_company')
                 .agg(F.min('date').alias('first_trans_date'))
                )

  gen_user_breakdown_df = (mainTable
                           .join(first_trans, ['unique_mem_id', 'clean_company'])
                            .withColumn('user_type', F.when(F.col('first_trans_date') == F.col('date'), 'New User')
                                        .otherwise(F.col('first_trans_date').cast('string'))
                                       )
                            .groupBy('clean_company', 'user_type', 'date')
                           .agg(F.countDistinct('unique_mem_id').alias('unique_users'),
                                F.sum('transactions').alias('transactions'),
                                F.sum('amount').alias('amount')
                               )                    
                            .join(baseUsersCount, ['date'])
                           .withColumn('amount', F.col('amount').cast(DoubleType()))
                           .withColumn('transactions', F.col('transactions').cast(DoubleType()))
                           .withColumn('unique_users', F.col('unique_users').cast(DoubleType()))
                            .withColumn('unique_users', F.col('unique_users') / F.col('unique_users_date'))
                            .withColumn('transactions', F.col('transactions') / F.col('unique_users_date'))
                            .withColumn('amount', F.col('amount') / F.col('unique_users_date'))
                            .withColumn('time_period', F.lit(time_period))
                            .drop('unique_users_date')
                           )
  return gen_user_breakdown_df

# COMMAND ----------

time_periods = ['month', 'year', 'quarter']
inputs = [cohortTable, baseUsersCount, baseGeoIncomeTagging]
time_period_dfs = gen_time_period_dfs(gen_user_breakdown, inputs, time_periods)
user_breakdown = combine_time_periods(time_period_dfs)

# COMMAND ----------

user_breakdown.write.mode('overwrite').format('delta').save(user_breakdown_path)

# COMMAND ----------

aurora_insight_upload(user_breakdown_path)

# COMMAND ----------

