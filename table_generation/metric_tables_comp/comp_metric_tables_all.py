# Databricks notebook source
from multiprocessing.pool import ThreadPool

# COMMAND ----------

# MAGIC %run "../helper_functions"

# COMMAND ----------

# comp_metrics = spark.table('insight.comp_metrics')
# crossover = spark.table('insight.crossover')
# retention = spark.table('insight.retention')
comp_state_inc_metrics = spark.read.format("delta").load(dbutils.widgets.get("comp_state_inc_metrics_table_path"))
# user_breakdown = spark.table('insight.user_breakdown')

# crossover_name = dbutils.widgets.get("notebook_paths").get("crossover")
# retention_name = dbutils.widgets.get("notebook_paths").get("retention")
# comp_metrics_name = dbutils.widgets.get("notebook_paths").get("comp_metrics")
comp_state_inc_metrics_name = dbutils.widgets.get("comp_state_inc_metrics_table_path")
# user_breakdown_name = dbutils.widgets.get("notebook_paths").get("crossover")

# comp_state_inc_metrics = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/comp_state_inc_metrics')
# comp_state_inc_metrics_name = 'dbfs:/mnt/mscience-dev/databases/insight/comp_state_inc_metrics'

clientName = dbutils.widgets.get("clientName")
# clientName = 'Shutterfly'
all_client_filters = spark.read.format("delta").load(dbutils.widgets.get("allClientFilters_table_path"))
# all_client_filters = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/allclientfiltersone')                          
clientFilter = (all_client_filters
                .where(F.col('client') == clientName)
                .collect()[0]['filters']
               )

# COMMAND ----------

dfs = {comp_state_inc_metrics_name: comp_state_inc_metrics}
#        comp_metrics_name: comp_metrics,
#        crossover_name: crossover, 
#        retention_name: retention,
#        user_breakdown_name: user_breakdown]

# COMMAND ----------

def gen_new_dfs(dfs, clientName):
  """Re-arrange a dictionary into a list of dictionaries"""
  new_dfs = []
  for name, df in dfs.items():
    this_dict = {}
    this_dict['name'] = name.replace('insight/', 'insight/' + clientName.replace(' ', '_').lower() + '_')
    this_dict['df'] = df
    this_dict['clientName'] = clientName
    new_dfs.append(this_dict)
  return new_dfs

# COMMAND ----------

dfs = gen_new_dfs(dfs, clientName)

# COMMAND ----------

def run_filter(dfs, clientFilter):
  """Run client filter on the dataframes contained in a list of dictionaries"""
  filtered_dfs = []
  for df_dict in dfs:
    new_df_dict = df_dict
    new_df_dict['df'] = new_df_dict['df'].where(clientFilter)
    filtered_dfs.append(new_df_dict)
  return filtered_dfs

# COMMAND ----------

def write_upload_table(table_dict):
  """Take a dictionary containing dataframe and write it to databricks and aurora"""
  df = table_dict['df']
  name = table_dict['name']
  print(name)
  df.write.mode('overwrite').format('delta').save(name)
  aurora_insight_upload(name)

# COMMAND ----------

def write_upload_metric_tables(clientName):
  """Generate client specific tables from larger metric tables
  
  Write each of the tables to databricks and aurora.
  """
  filtered_dfs = run_filter(dfs, clientFilter)
  pool = ThreadPool()
  # Write and upload each of the tables in parallel
  pool.map(write_upload_table, filtered_dfs)

# COMMAND ----------

write_upload_metric_tables(clientName)

# COMMAND ----------

k = spark.read.format("delta").load('dbfs:/mnt/mscience-dev/databases/insight/shutterfly_comp_state_inc_metrics')

# COMMAND ----------

display(k)

# COMMAND ----------

