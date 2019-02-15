# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

def get_years_inbetween(startDate, endDate):
  """Return a list of years as ints between and including two dates"""
  yearList = []
  startYear = startDate.year
  endYear = endDate.year
  for addYear in range(int(endYear - startYear + 1)):
    thisYear = startYear + addYear
    yearList.append(thisYear)
  return yearList

# COMMAND ----------

def get_dates_inbetween(startDate, endDate):
  """Return a list of dates between (chronologically) and including two dates
  
  Dates returned in the list are datetime.date objects
  """
  daterange = []
  for n in range(int((endDate - startDate).days) + 1):
    newDate = startDate + datetime.timedelta(n)
    daterange.append(newDate)
  return daterange

# COMMAND ----------

def year_list_to_string(years):
  """Take a list of years as ints and outputs a string containing all those years separated by /"""
  years_string = ''
  for year in years:
    years_string = years_string + str(year) + "/"
  years_string = years_string[:-1]
  return years_string

# COMMAND ----------

allClientHolidaysRange = spark.table(dbutils.widgets.get("allClientHolidaysRange_table_path"))
allClientHolidaysPath = dbutils.widgets.get("allClientHolidays_table_path")
baseTable = spark.table(dbutils.widgets.get("baseTable_table_path"))

# allClientHolidaysRange = spark.table('insight.allClientHolidaysRange')
# allClientHolidaysPath = 'insight.allClientHolidays'
# baseTable = spark.table('insight.baseTable')

# COMMAND ----------

# sql("drop table if exists {}".format(allClientHolidaysPath))
# dbutils.fs.rm("dbfs:/mnt/mscience-dev/databases/'insight/allClientHolidays'",True)

# COMMAND ----------

# allClientHolidaysRange = [Row(client = 'Health_and_Fitness', holiday = 'Thanksgiving', start = '1 week before Thanksgiving', end = '1 week after Thanksgiving'),
#                           Row(client = 'Health_and_Fitness', holiday = 'Christmas', start = '1 week before Christmas', end = '1 week after Christmas'),
#                           Row(client = 'Outdoors Retail', holiday = 'Thanksgiving', start = '1 week before Thanksgiving', end = '1 week after Thanksgiving'),
#                           Row(client = 'Outdoors Retail', holiday = 'Christmas', start = '1 week before Christmas', end = '1 week after Christmas')
#                          ]
# allClientHolidaysRange = sc.parallelize(allClientHolidaysRange).toDF()
# write_table_b(allClientHolidaysRange, 'insight.allClientHolidaysRange')

# COMMAND ----------

# allClientFilters = [Row(client = 'Health_and_Fitness', filters = "InsightVertical = 'Health_and_Fitness'"),
#                     Row(client = 'Outdoors Retail', filters = "InsightSubvertical = 'Outdoors Retail'")
#                    ]
# allClientFilters = sc.parallelize(allClientFilters).toDF()
# write_table_b(allClientFilters, 'insight.allClientFilters')

# COMMAND ----------

# Turn allClientHolidaysRange into a list
allClientHolidaysRange = allClientHolidaysRange.collect()

# COMMAND ----------

maxDate = (baseTable
           .groupBy()
           .agg(F.max('date').alias('maxDate'))
          ).collect()[0]['maxDate']

# COMMAND ----------

minDate = (baseTable
           .groupBy()
           .agg(F.min('date').alias('minDate'))
          ).collect()[0]['minDate']

# COMMAND ----------

years = get_years_inbetween(minDate, maxDate)

# COMMAND ----------

def gen_allHolidayDays(holidayRanges, years):
  """Return all dates contained in each of the holidays in allHolidayRanges
  
  A list of row objects is returned, where each row object contains the client, holiday, and
  date in that holiday. Holidays are assumed to repeat each year at the same time, start
  in the year designated by the holiday start date and end in the last year of the data.
  """
  allHolidayDays = []
  for holidayRange in holidayRanges:
    for year in years:
      start = holidayRange['start'] + " " + str(year)
      end = holidayRange['end'] + " " + str(year)
      startDate = DateParser(start).result()[0].date()
      endDate = DateParser(end).result()[0].date()
      dateRange = get_dates_inbetween(startDate, endDate)
      years_list = get_years_inbetween(startDate, endDate)
      years_string = year_list_to_string(years_list)
      for date in dateRange:
        newRow = Row(client = holidayRange['client'],
                     holiday = holidayRange['holiday'],
                     date = date,
                     years = years_string
                    )
        allHolidayDays.append(newRow)
  return allHolidayDays

# COMMAND ----------

allClientHolidays = gen_allHolidayDays(allClientHolidaysRange, years)
# Turn allClientHolidays into a dataframe
allClientHolidays = sc.parallelize(allClientHolidays).toDF()

# COMMAND ----------

allClientHolidays.write.mode('overwrite').format('delta').save(allClientHolidaysPath)