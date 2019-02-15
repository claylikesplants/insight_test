# Databricks notebook source
from pyspark.sql import Row
import uuid
import time
import pyspark.sql.functions as F

# COMMAND ----------

def append_table(appendee_table_name, appender_table_name):
  appendee_table = spark.table(appendee_table_name)
  appender_table = spark.table(appender_table_name)
  combined_table = (appendee_table
                    .union(appender_table)
                    .distinct()
                   )
  temp_table_name = appender_table_name + str(uuid.uuid4()).replace("-", "_")
  combined_table.write.mode('overwrite').saveAsTable(temp_table_name)
  time.sleep(10)
  temp_table = spark.table(temp_table_name)
  temp_table.write.mode('overwrite').saveAsTable(appendee_table_name)
  sql("drop table if exists " + temp_table_name)
  sql("drop table if exists " + appender_table_name)

# COMMAND ----------

def change_client_names(table_name_to_modify, name_change_dict):
  table_to_modify = spark.read.format('delta').load(table_name_to_modify)
  name_change_rows = []
  for old_name, new_name in name_change_dict.items():
    name_change_row = Row(client = old_name, new_client = new_name)
    name_change_rows.append(name_change_row)
  name_change_df = sc.parallelize(name_change_rows).toDF()
  new_table = (table_to_modify
               .join(name_change_df, ['client'], 'left')
               .withColumn('new_client', F.when(F.isnull('new_client'), F.col('client'))
                           .otherwise(F.col('new_client'))
                          )
               .drop('client')
               .withColumnRenamed('new_client', 'client')
              )
  new_table.write.mode('overwrite').format('delta').save(table_name_to_modify)

# COMMAND ----------

def replace_table(table_name_to_be_replaced, replacer_table_name):
  replacer_table = spark.table(replacer_table_name)
  replacer_table.write.mode('overwrite').saveAsTable(table_name_to_be_replaced)
  sql("drop table if exists " + replacer_table_name)

# COMMAND ----------

def add_to_all_client_holidays_range(additional_all_client_holiday_ranges_table_name):
  additional_all_client_holidays_range = spark.table(additional_all_client_holidays_range_table_name)
  current_all_client_holidays_range = spark.table('insight.allclientholidaysrange')
  all_client_holiday_ranges = (additional_all_client_holidays_range
                               .join(current_all_client_holidays_range, ['client'], 'outer')
                              )
  write_table_c(all_client_holiday_ranges, 'insight.allclientholidaysrange')

# COMMAND ----------

def remove_clients_from_filters_and_holidays(clients_to_remove):
  clients_to_remove_row_list = []
  for client in clients_to_remove:
    clients_to_remove_row_list += Row(client = client)
  clients_to_remove_df = sc.parallelize(clients_to_remove_row_list).toDF()
  current_all_client_filters = spark.table('insight.allclientfilters')
  current_all_client_holidays_range = spark.table('insight.allclientholidaysrange')
  current_all_client_filters = (current_all_client_filters
                                 .join(clients_to_remove_df, ['client'], 'anti-left')
                                )
  new_all_client_holidays_range = (new_all_client_holidays_range
                                   .join(clients_to_remove_df, ['client'], 'anti-left')
                                  )
  write_table_c(current_all_client_filters, 'insight.allclientfilters')
  write_table_c(current_all_client_filters, 'insight.allclientholidaysrange')

# COMMAND ----------

def remove_client_metric_tables(clients_to_remove):
  insight_files = dbutils.fs.ls("dbfs:/mnt/mscience-dev/databases/insight")
  for client in clients_to_remove:
    for file in insight_files:
      search_phrase = client.replace(" ", "_")
      if client in file['name']:
        dbutils.fs.rm(file['path'], True)

# COMMAND ----------

def add_to_all_client_filters(additional_all_client_filters_table_name):
  additional_all_client_filters = spark.table(additional_all_client_filters_table_name)
  current_all_client_filters = spark.table('insight.allclientfilters')
  all_client_filters = (current_all_client_filters
                         .join(additional_all_client_filters, ['client'], 'outer')
                        )

# COMMAND ----------

def gen_client_metric_tables(client_attribute_table_paths):
  dbutils.notebook.run("./supporting_notebooks/tables_all", 30000, client_attribute_table_paths)
  dbutils.notebook.run("./supporting_notebooks/tables_comp", 100000, client_attribute_table_paths)    

# COMMAND ----------

# def modify_filters(client_filter_mapping_dict):
#   all_client_filters_rows = (spark.table('insight.allclientfilters')
#                              .where(F.col('client') not in )
#                              .collect()
#                             )
#   for clients, filter in client_filter_mapping_dict:
    

# COMMAND ----------

# manipulate here

# COMMAND ----------

append_table('insight.allclientholidaysrange', 'insight.append_holidays_temp')

# COMMAND ----------

replace_table('insight.allclientholidaysrange', 'insight.append_holidays')

# COMMAND ----------

change_names = {
  "Online_Consignment": "The RealReal",
  "Outdoors_Retail": "Backcountry",
  "Shuterfly": "Shutterfly",
  "Health_and_Fitness": "Health and fitness"
}

# COMMAND ----------

change_client_names('insight.allclientfilters', change_names)

# COMMAND ----------

t = (spark.table('insight.basetable')
     .withColumnRenamed('clean_company', 'company')
     .withColumnRenamed('InsightVertical', 'vertical')
     .withColumnRenamed('InsightSubvertical', 'sub_vertical')
    )
t.write.mode('overwrite').format('delta').partitionBy('unique_mem_id', 'date', 'company').save('dbfs:/mnt/mscience-dev/databases/insight/base_table_optimized')

# COMMAND ----------

