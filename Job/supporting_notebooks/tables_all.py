# Databricks notebook source
from multiprocessing.pool import ThreadPool

# COMMAND ----------

# MAGIC %run "./helper_funs_dicts"

# COMMAND ----------

client_table_locations = {"allClientFilters_table_path": dbutils.widgets.get("allClientFilters_table_path"),
                          "allClientHolidaysRange_table_path": dbutils.widgets.get("allClientHolidaysRange_table_path")
                         }
table_paths_all = merge_dicts(table_paths_all, client_table_locations)

# COMMAND ----------

notebook_order = [['baseGeoIncomeTagging'],
                  ['baseUsersCount', 'allClientHolidays', 'dayNum'],
                   ['comp_state_inc_metrics']
                      ]

notebook_order_paths = gen_notebook_order_path(notebook_order, notebook_paths)

# COMMAND ----------

pass_in = merge_dicts(notebook_paths, table_paths_all)

# COMMAND ----------

# dbutils.notebook.run('/GROUPS/mr/Insight/clays_functions/base_tables_comp/txnTimeSpan', 3600, pass_in)

# COMMAND ----------

for notebook_group in notebook_order_paths:
  pool = ThreadPool()
  pool.map(
    lambda notebook_path: dbutils.notebook.run(
      notebook_path,
      timeout_seconds = 7200,
      arguments = pass_in
    ), notebook_group)

# COMMAND ----------

