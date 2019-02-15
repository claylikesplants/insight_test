# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

baseTable = spark.table(dbutils.widgets.get("baseTable_table_path"))
clientName = dbutils.widgets.get("clientName")
cohortTablePath = dbutils.widgets.get("cohortTable_table_path")
allClientFilters = spark.read.format("delta").load(dbutils.widgets.get("allClientFilters_table_path"))

# baseTable = spark.table('insight.baseTable')
# clientName = 'The RealReal'
# cohortTablePath = 'insight.the_realreal_outlet_cohorttable'
# allClientFilters = spark.table('insight.allClientFiltersone')

# COMMAND ----------

# Turn filter dataframe into list
filterTable = allClientFilters.where("client = '" + clientName + "'") 
filters = filterTable.select(F.col('filters')).rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# Run filters on baseTable
cohortTable = baseTable
for single_filter in filters:
  cohortTable = cohortTable.where(single_filter).distinct()

# COMMAND ----------

cohortTable.write.mode('overwrite').format('delta').save(cohortTablePath)