# Databricks notebook source
job_inputs = {"allClientFilters_table_path": "dbfs:/mnt/mscience-dev/databases/insight/allclientfiltersone",
              "allClientHolidaysRange_table_path": "insight.allclientholidaysrangeone"
             }

# COMMAND ----------

dbutils.notebook.run("./supporting_notebooks/tables_all", 30000, job_inputs)

# COMMAND ----------

dbutils.notebook.run("./supporting_notebooks/tables_comp", 100000, job_inputs)

# COMMAND ----------

