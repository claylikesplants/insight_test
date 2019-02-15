# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

t = (spark.table('insight.basetable')
     .withColumnRenamed('clean_company', 'company')
     .withColumnRenamed('InsightVertical', 'vertical')
     .withColumnRenamed('InsightSubvertical', 'sub_vertical')
    )
t.write.mode('overwrite').format('delta').partitionBy('date').save('dbfs:/mnt/mscience-dev/databases/insight/base_table_target_optimized')

# COMMAND ----------

k = (spark.table('insight.basetable')
     .withColumnRenamed('clean_company', 'company')
     .withColumnRenamed('InsightVertical', 'vertical')
     .withColumnRenamed('InsightSubvertical', 'sub_vertical')
    )
k.write.mode('overwrite').format('delta').save('dbfs:/mnt/mscience-dev/databases/insight/base_table_target')

# COMMAND ----------

p = spark.read.format('delta').load('dbfs:/mnt/mscience-dev/databases/insight/base_table_target_optimized')
l = spark.read.format('delta').load('dbfs:/mnt/mscience-dev/databases/insight/base_table_target')

# COMMAND ----------

p1 = (p
      .groupBy('date', 'unique_mem_id')
      .agg(F.sum('transactions').alias('transactions'))
     )
display(p1)

# COMMAND ----------

l1 = (l
      .groupBy('date', 'unique_mem_id')
      .agg(F.sum('transactions').alias('transactions'))
     )
display(l1)

# COMMAND ----------

l = spark.table('insight.basetable')
l.write.mode('overwrite').saveAsTable('insight.claybasetable')

# COMMAND ----------

