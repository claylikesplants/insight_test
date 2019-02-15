# Databricks notebook source
import datetime
from pyspark.sql import Row

# COMMAND ----------

def merge_dicts(*dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

# COMMAND ----------

def gen_notebook_path(notebook_folders):
  notebook_paths = {}
  for notebook, notebook_folder in notebook_folders.items():
    if notebook != 'helper_functions':
      notebook_path = '../../table_generation/' + notebook_folder + '/' + notebook
    else:
      notebook_path = '../../Insight/table_generation/' + notebook
    notebook_paths[notebook + '_notebook_path'] = notebook_path
  return notebook_paths

# COMMAND ----------

def gen_table_path_all(table_names):
  table_paths = {}
  for table_name in table_names:
    table_paths[table_name + '_table_path'] = "dbfs:/mnt/mscience-dev/databases/insight/" + table_name
  return table_paths

# COMMAND ----------

def gen_notebook_order_path(notebook_order, notebook_paths):
  notebook_order_path = []
  for notebook_group in notebook_order:
    notebook_group_path = []
    for notebook in notebook_group:
      notebook_group_path.append(notebook_paths[notebook + '_notebook_path'])
    notebook_order_path.append(notebook_group_path)
  return notebook_order_path

# COMMAND ----------

table_names_comp = [
  'cohortTable', 
 'holidayCohortTable', 
 'holidayDayCount', 
 'holidayUsersCount', 
 'holiday_metrics', 
 'transaction_bins',
  'comp_metrics',
   'crossover',
   'retention',
   'user_breakdown',
  'holiday_metrics',
  'transaction_bins',
  'user_breakdown',
  'txnTimeSpan',
  'comp_state_inc_metrics'
]

# COMMAND ----------

table_names_all = [
   'comp_state_inc_metrics',
  'baseTable',
  'baseUsersCount',
  'dayNum',
  'state_abr_to_name',
  'baseGeoIncomeTagging',
  'allClientHolidays',
  'income_order',
  'baseGeoIncomeTagging',
  'state_to_region'
]

table_paths_all = gen_table_path_all(table_names_all)
table_paths_all['baseTable_table_path'] = 'insight.baseTable'

# COMMAND ----------

# allClientFilters = [Row(client = 'Health_and_Fitness', filters = "InsightVertical = 'Health_and_Fitness'"),
#                     Row(client = 'Outdoors Retail', filters = "InsightSubvertical = 'Outdoors Retail'")
#                    ]
# allClientFilters = sc.parallelize(allClientFilters).toDF()
# allClientFilters.write.saveAsTable(table_paths_all['allClientFilters_table_path'], method = 'overwrite')

# COMMAND ----------

# allClientHolidaysRange = [Row(client = 'Health_and_Fitness', holiday = 'Thanksgiving', start = '1 week before Thanksgiving', end = '1 week after Thanksgiving'),
#                           Row(client = 'Health_and_Fitness', holiday = 'Christmas', start = '1 week before Christmas', end = '1 week after Christmas'),
#                           Row(client = 'Outdoors Retail', holiday = 'Thanksgiving', start = '1 week before Thanksgiving', end = '1 week after Thanksgiving'),
#                           Row(client = 'Outdoors Retail', holiday = 'Christmas', start = '1 week before Christmas', end = '1 week after Christmas')
#                          ]
# allClientHolidaysRange = sc.parallelize(allClientHolidaysRange).toDF()
# allClientHolidaysRange.write.saveAsTable(table_paths_all['allClientHolidaysRange_table_path'], method = 'overwrite')

# COMMAND ----------

notebook_folders = {
   'cohortTable': 'base_tables_comp',
   'holidayCohortTable': 'base_tables_comp',
   'holidayDayCount': 'base_tables_comp',
   'holidayUsersCount': 'base_tables_comp',
  'comp_metrics': 'metric_tables_comp',
  'comp_state_inc_metrics': 'metric_tables_all',
  'crossover': 'metric_tables_comp',
  'retention': 'metric_tables_comp',
  'user_breakdown': 'metric_tables_comp',
  'holiday_metrics': 'metric_tables_comp',
   'transaction_bins': 'metric_tables_comp',
  'baseTable': 'base_tables_all',
  'baseUsersCount': 'base_tables_all',
  'dayNum': 'base_tables_all',
  'state_abr_to_name': 'base_tables_all',
  'txnTimeSpan': 'base_tables_comp',
  'users_state_inc': 'base_tables_all',
  'allClientHolidays': 'base_tables_all',
  'comp_metric_tables_all': 'metric_tables_comp',
  'helper_functions': '',
  'baseGeoIncomeTagging': 'base_tables_all',
  'state_to_region': 'base_tables_all'
}

notebook_paths = gen_notebook_path(notebook_folders)

# COMMAND ----------

