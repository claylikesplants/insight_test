# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

income_order_path = dbutils.widgets.get("income_order_table_path")
baseGeoIncomeTaggingPath = dbutils.widgets.get("baseGeoIncomeTagging_table_path")
state_abr_to_name = spark.read.format("delta").load(dbutils.widgets.get("state_abr_to_name_table_path"))
state_to_region = spark.read.format("delta").load(dbutils.widgets.get("state_to_region_table_path"))

# income_order_path = 'insight.income_order'
# baseGeoIncomeTaggingPath = 'insight.baseGeoIncomeTagging'
# state_abr_to_name_path = dbutils.widgets.get("baseGeoIncomeTagging_table_path")

# COMMAND ----------

income_to_income_cat_list = [[0, '<25k'], 
                             [25000, '25k-35k'], 
                             [35000, '35k-50k'], 
                             [50000, '50k-75k'], 
                             [75000, '75k-100k'], 
                             [100000, '100k-150k'], 
                             [150000, '150k-200k'], 
                             [200000, '>200k']]

# COMMAND ----------

def income_to_census_cat(income):
  """Assign a given income level to a category using the above dictionary"""
  i = 0
  while income > income_to_income_cat_list[i][0]:
    i += 1
    if i > (len(income_to_income_cat_list) - 1):
      return income_to_income_cat_list[i-1][1]
  return income_to_income_cat_list[i-1][1]

income_to_census_cat_udf = F.udf(income_to_census_cat, StringType())

# COMMAND ----------

income_order = sc.parallelize(income_to_income_cat_list).toDF(['income_order', 'census_cat'])

# COMMAND ----------

rolling_income_12_months = (spark.table('group_dse.rolling_income_12month')
                            .select('unique_mem_id', 'month_year', 'income_12_months')
                            .withColumnRenamed('month_year', 'date')
                             .withColumnRenamed('income_12_months', 'income')
                           )

# COMMAND ----------

geousers = spark.table('nile.geousers_v2_monthly')
geousers = (geousers
            .join(state_abr_to_name.select('state_abr', F.col('state').alias('state1_full')), geousers.state1 == state_abr_to_name.state_abr, 'inner')
           .drop('state_abr')
           .join(state_abr_to_name.select('state_abr', F.col('state').alias('state2_full')), geousers.state2 == state_abr_to_name.state_abr, 'inner')
           .drop('state_abr')
            # Convert month and year to a date
           .withColumn('year', F.col('year').cast(StringType()))
           .withColumn('month', F.col('month').cast(StringType()))
           .withColumn('month', F.lpad(F.col('month'), 2, '0'))
           .withColumn('date_string', F.concat_ws('-', F.col('year'), F.col('month'), F.lit('01')))
           .withColumn('date', F.to_date('date_string'))
           .withColumnRenamed('state1_full', 'state')
            .join(state_to_region, ['state'])
           .select('unique_mem_id', 'date', 'state', 'region')
           )

# COMMAND ----------

def gen_users_state_inc_date(income_df, state_df, time_period):
  users_inc_date_df = (income_df
                       .withColumn('date', F.date_trunc(time_period, F.col('date')))
                       .groupBy('unique_mem_id', 'date')
                       .agg(F.avg('income').alias('income'))
                       .withColumn('census_cat', income_to_census_cat_udf(F.col('income')))
                       .withColumnRenamed('census_cat', 'income')
                       .withColumn('time_period', F.lit(time_period))
                       .drop('income')
                      )
  w = Window().partitionBy('unique_mem_id', 'date').orderBy(F.col('state_count').desc())
  users_state_date_df = (state_df
                        .withColumn('date', F.date_trunc(time_period, F.col('date')))
                         .groupBy('unique_mem_id', 'date', 'state', 'region')
                         .agg(F.count('state').alias('state_count'))
                         # Find the state with the maximum occurances in the period
                         # by ranking them by person and time period
                         .withColumn('row_num', F.row_number().over(w))
                         .where(F.col('row_num') == 1)
                         .drop('row_num', 'state_count')
                        )
  # Join geo and income information together
  users_state_inc_date_df = (users_inc_date_df
                             .join(users_state_date_df, ['unique_mem_id', 'date'])
                             .withColumn('time_period', F.lit(time_period))
                            )
  return users_state_inc_date_df

# COMMAND ----------

time_periods = ['day', 'month', 'year', 'quarter']
inputs = [rolling_income_12_months, geousers]
time_period_dfs = gen_time_period_dfs(gen_users_state_inc_date, inputs, time_periods)
baseGeoIncomeTagging = combine_time_periods(time_period_dfs)

# COMMAND ----------

baseGeoIncomeTagging.write.mode('overwrite').format('delta').save(baseGeoIncomeTaggingPath)

# COMMAND ----------

income_order = sc.parallelize(income_to_income_cat_list).toDF(['income_order', 'census_cat'])
income_order.write.mode('overwrite').format('delta').save(income_order_path)

# COMMAND ----------

