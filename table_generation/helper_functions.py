# Databricks notebook source
import EgressManager_v1 as egress
import pyspark.sql.functions as F
from pyspark.sql.types import *
import datetime
import math
from pyspark.sql.window import Window
from pyspark.sql.functions import pandas_udf, PandasUDFType
import sys
from multiprocessing.pool import ThreadPool
import re
import time
import uuid
from pyspark.sql import Row
from natty import DateParser
import socket
socket.setdefaulttimeout(None)

# COMMAND ----------

sql("SET spark.databricks.io.directoryCommit.enableLogicalDelete=true")

# COMMAND ----------

def gen_time_period_df(inputs):
  """Takes a list, removes the function at the end and runs the function on the remaining inputs"""
  fun = inputs[-1]
  del inputs[-1]
  return fun(*inputs)

# COMMAND ----------

def gen_time_period_dfs(gen_time_period_fun, gen_time_period_fun_inputs, time_periods):
  """Runs a function that generates dataframes for multiple time periods in parallel"""
  inputs_list = []
  for time_period in time_periods:
    time_period_inputs = gen_time_period_fun_inputs + [time_period] + [gen_time_period_fun]
    inputs_list.append(time_period_inputs)
  pool = ThreadPool()
  time_period_dfs = pool.map(gen_time_period_df, inputs_list)
  return time_period_dfs

# COMMAND ----------

def gen_time_period_dfs_r(gen_time_period_fun, gen_time_period_fun_inputs, time_periods, methods):
  """Runs a function that generates dataframes for multiple time periods and methods in parallel"""
  inputs_list = []
  for time_period in time_periods:
    for method in methods:
      time_period_method_inputs = gen_time_period_fun_inputs + [time_period] + [method] + [gen_time_period_fun]
      inputs_list.append(time_period_method_inputs)
  pool = ThreadPool()
  time_period_dfs = pool.map(gen_time_period_df, inputs_list)
  return time_period_dfs

# COMMAND ----------

def combine_time_periods(time_period_dfs):
  """Union more than 2 data frames"""
  first = True
  for time_period_df in time_period_dfs:
    if first:
      time_periods_union = time_period_df
      first = False
    else:
      time_periods_union = (time_periods_union
                            .union(time_period_df)
                           )
  return time_periods_union

# COMMAND ----------

def aurora_insight_upload(source_table_name):
  """Upload spark table to aurora"""
  dest_table_name = source_table_name.replace('dbfs:/mnt/mscience-dev/databases/insight/', '')
  secret_scope = "prod_aurora_insight"
  dest_database = "insight_v1"
  em = egress.EgressManager(spark, dbutils, secret_scope, dest_database)
  source_table = spark.read.format("delta").load(source_table_name)
  em.perform_etl_overwrite(source_table, dest_table_name)

# COMMAND ----------

def write_table(df, tableName):
  sql("drop table if exists {}".format(tableName))
  newTableName = tableName.replace(".", "/")
  databaseTableName = ("dbfs:/mnt/mscience-dev/databases/" + newTableName).lower()
  dbutils.fs.rm(databaseTableName, True)
  viewName = "tempView_" + str(uuid.uuid4()).replace("-", "_")
  df.createOrReplaceTempView(viewName)
#   time.sleep(30)
  try:
    sql("insert overwrite table {} select * from {}".format(tableName, viewName))
#   except AnalysisException:
#     print('hello')
  except Exception as e:
    if 'Table' in str(e):
      print('hello')
      spark.createDataFrame(sc.emptyRDD(), df.schema).write.saveAsTable(tableName)
      sql("insert overwrite table {} select * from {}".format(tableName, viewName))
      sql("refresh table {}".format(tableName))
#     spark.createDataFrame(sc.emptyRDD(), df.schema).write.saveAsTable(tableName)
#     sql("insert overwrite table {} select * from {}".format(tableName, viewName))
#   finally:
#     sql("drop table if exists {}".format(viewName))
#     sql("refresh table {}".format(tableName))

# COMMAND ----------

def write_table_b(df, tableName):
  try:
    df.write.saveAsTable(tableName, mode = 'overwrite')
    time.sleep(10)
    sql("refresh table {}".format(tableName))
  except Exception as e:
    exceptionMessage = str(e)
    print("Error Message:" + "\n" + exceptionMessage + "\n")
    if 'An error occurred while calling' in exceptionMessage:
      sql("drop table if exists {}".format(tableName))
      time.sleep(10)
      write_table_b(df, tableName)
    if 'Can not create the managed table' in exceptionMessage:
      sql("drop table if exists {}".format(tableName))
      newTableName = tableName.replace(".", "/")
      databaseTableName = ("dbfs:/mnt/mscience-dev/databases/" + newTableName).lower()
      dbutils.fs.rm(databaseTableName, True)
      time.sleep(10)
      write_table_b(df, tableName)
    if 'already exists in database' in exceptionMessage:
      sql("drop table if exists {}".format(tableName))
      time.sleep(10)
      write_table_b(df, tableName)

# COMMAND ----------

def write_table_c(df, tableName):
  sql("drop table if exists {}".format(tableName))
  newTableName = tableName.replace(".", "/")
  databaseTableName = ("dbfs:/mnt/mscience-dev/databases/" + newTableName).lower()
  dbutils.fs.rm(databaseTableName, True)
  time.sleep(10)
  df.write.saveAsTable(tableName, mode = 'overwrite')
  time.sleep(10)
  sql("refresh table {}".format(tableName))
  time.sleep(30)

# COMMAND ----------

def write_table2(df, tableName):
  try:
    df.write.saveAsTable(tableName, mode = 'overwrite')
  except Py4JError:
    write_table2(df, tableName)
  except:
    newTableName = tableName.replace(".", "/")
    databaseTableName = ("dbfs:/mnt/mscience-dev/databases/" + newTableName).lower()
    dbutils.fs.rm(databaseTableName, True)
    time.sleep(30)
    df.write.saveAsTable(tableName, mode = 'overwrite')
  finally:
    sql("refresh table {}".format(tableName))

# COMMAND ----------

def remove_table_files(tableName):
  newTableName = tableName.replace(".", "/")
  databaseTableName = ("dbfs:/mnt/mscience-dev/databases/" + newTableName).lower()
  dbutils.fs.rm(databaseTableName, True)

# COMMAND ----------

def check_table_exists_in_fs(tableName):
  split_table_name = tableName.split(".")
  db_file_path = ("dbfs:/mnt/mscience-dev/databases/" + split_table_name[0]).lower()
  file_name = (split_table_name[1] + "/").lower()
  file_info_objects = dbutils.fs.ls(db_file_path)
  file_names = [file_info_object.name for file_info_object in file_info_objects]
  if file_name in file_names:
    return True
  else:
    return False

# COMMAND ----------

def check_table_exists_in_db(tableName):
  split_table_name = tableName.split(".")
  db_name = split_table_name[0]
  table_name = split_table_name[1]
  if table_name in sqlContext.tableNames(db_name):
      return True
  else:
    return False

# COMMAND ----------

def drop_table_from_db(tableName):
  sql("drop table if exists " + tableName)
  time.sleep(5)
  if check_table_exists_in_db(tableName):
    drop_table_from_db(tableName)
    print("table wasn't removed from db properly")

# COMMAND ----------

def drop_table_from_fs(tableName):
  remove_table_files(tableName)
  if check_table_exists_in_fs(tableName):
    drop_table_from_fs(tableName)
    print("table wasn't removed from fs properly")
  time.sleep(30)

# COMMAND ----------

def drop_table(tableName):
  drop_table_from_db(tableName)
  drop_table_from_fs(tableName)

# COMMAND ----------

def try_overwrite(df, tableName):
  try:
    df.write.saveAsTable(tableName, mode = 'overwrite')
    sql("refresh table {}".format(tableName))
    return True
  except Exception as e:
    exceptionMessage = str(e)
    print("Error Message:" + "\n" + exceptionMessage + "\n")
    if 'Can not create the managed table' in exceptionMessage:
      remove_table_files(tableName)
    if 'already exists in database' in exceptionMessage:
      sql("drop table if exists {}".format(tableName))
  return False

# COMMAND ----------

def overwrite(df, tableName):
  written = False
  tries = 0
  while not written or tries > 10:
    written = try_overwrite(df, tableName)
    tries += 1

# COMMAND ----------

