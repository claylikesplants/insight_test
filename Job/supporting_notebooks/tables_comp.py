# Databricks notebook source
from multiprocessing.pool import ThreadPool
from pyspark.sql import Row
import datetime

# COMMAND ----------

# MAGIC %run "./helper_funs_dicts"

# COMMAND ----------

client_table_locations = {"allClientFilters_table_path": dbutils.widgets.get("allClientFilters_table_path"),
                          "allClientHolidaysRange_table_path": dbutils.widgets.get("allClientHolidaysRange_table_path")
                         }
table_paths_all = merge_dicts(table_paths_all, client_table_locations)

# COMMAND ----------

clientList = spark.read.format("delta").load(dbutils.widgets.get("allClientFilters_table_path")).select('client').distinct().collect()
clientList = [Row['client'] for Row in clientList]

# COMMAND ----------

def gen_table_path_comp(table_names, clientName):
  clientName = clientName.replace(" ", "_")
  table_paths = {}
  for table_name in table_names:
    table_path = "dbfs:/mnt/mscience-dev/databases/insight/" + clientName.lower() + '_' + table_name
    table_paths[table_name + '_table_path'] = table_path
  return table_paths

# COMMAND ----------

notebook_order = [['cohortTable'],
                    ['holidayCohortTable', 'txnTimeSpan'],
                    ['holidayDayCount', 'holidayUsersCount'],
                    ['comp_metric_tables_all', 'holiday_metrics', 'transaction_bins', 'crossover', 'comp_metrics', 'retention', 'user_breakdown']
                   ]

notebook_order_paths = gen_notebook_order_path(notebook_order, notebook_paths)

# COMMAND ----------

def write_upload_tables(notebook_order_paths, pass_in):
  for notebook_group in notebook_order_paths:
    pool = ThreadPool(8)
    pool.map(
      lambda notebook: dbutils.notebook.run(
        notebook,
        timeout_seconds = 20000,
        arguments = pass_in
      ),
      notebook_group
    )

# COMMAND ----------

def gen_client_tables(client):
  table_paths_comp = gen_table_path_comp(table_names_comp, client)
  client_dict = {'clientName': client}
  pass_in = merge_dicts(client_dict, notebook_paths, table_paths_comp, table_paths_all)
  write_upload_tables(notebook_order_paths, pass_in)

# COMMAND ----------

bigPool = ThreadPool(1)
bigPool.map(gen_client_tables, clientList)