# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

baseTable = spark.table(dbutils.widgets.get("baseTable_table_path"))
dayNumPath = dbutils.widgets.get("dayNum_table_path")

# baseTable = spark.table('insight.baseTable')
# dayNumPath = "insight.dayNum"

# COMMAND ----------

days_in_month = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

# COMMAND ----------

days_in_quarter = {1:90, 2:91, 3:92, 4:92}

# COMMAND ----------

def num_days_in_month(date):
  """Find the number of days in a given month"""
  year = int(date.year)
  month = int(date.month)
  if ((year % 4 == 0) and (month == 2)):
    days = 29
  else:
    days = days_in_month[month]
  return days

udf_num_days_in_month = F.udf(num_days_in_month, IntegerType())

# COMMAND ----------

def num_days_in_quarter(date):
  """Find the number of days in a given quarter"""
  quarter = int(math.ceil(date.month / 3.))
  year = int(date.year)
  if ((year % 4 == 0) and (quarter == 1)):
    days = 91
  else:
    days = days_in_quarter[quarter]
  return days

udf_num_days_in_quarter = F.udf(num_days_in_quarter, IntegerType())

# COMMAND ----------

def num_days_in_year(date):
  """Find the number of days in a given year"""
  year = int(date.year)
  if (year % 4 == 0):
    days = 366
  else:
    days = 365
  return days

udf_num_days_in_year = F.udf(num_days_in_year, IntegerType())

# COMMAND ----------

def gen_time_periods(baseTable, time_period):
  time_periods = (baseTable
                  .withColumn('date', F.date_trunc(time_period, 'date'))
                  .select('date')
                  .distinct()
                  .withColumn('time_period', F.lit(time_period))
                 )
  return time_periods

# COMMAND ----------

# Find number of days in each time period
months = (gen_time_periods(baseTable, 'month')
          .withColumn('days', udf_num_days_in_month(F.col('date')))
          )
quarters = (gen_time_periods(baseTable, 'quarter')
            .withColumn('days', udf_num_days_in_quarter(F.col('date')))
           )
years = (gen_time_periods(baseTable, 'year')
         .withColumn('days', udf_num_days_in_year(F.col('date')))
        )
time_period_dfs = [months, quarters, years]
dayNum = combine_time_periods(time_period_dfs)

# COMMAND ----------

dayNum.write.mode('overwrite').format('delta').save(dayNumPath)