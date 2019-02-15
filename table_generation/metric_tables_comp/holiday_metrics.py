# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

holidayCohortTable = spark.read.format("delta").load(dbutils.widgets.get("holidayCohortTable_table_path"))
holidayUsersCount = spark.read.format("delta").load(dbutils.widgets.get("holidayUsersCount_table_path"))
holidayDayCount = spark.read.format("delta").load(dbutils.widgets.get("holidayDayCount_table_path"))
holiday_metrics_path = dbutils.widgets.get("holiday_metrics_table_path")

# holidayCohortTable = spark.table('insight.shutterfly_holidayCohortTable')
# holidayUsersCount = spark.table('insight.shutterfly_holidayUsersCount')
# holidayDayCount = spark.table('insight.shutterfly_holidayDayCount')
# holiday_metrics_path = 'insight.shutterfly_holiday_metrics'

# COMMAND ----------

# Generate transactions per user, spend per user, average
# transaction, spend, and transactions for a list of holidays
holiday_metrics = (holidayCohortTable
                   .groupBy('holiday', 'clean_company', 'insightVertical', 'insightSubvertical', 'years')
                   .agg(F.sum('transactions').alias('transactions'),
                         F.sum('amount').alias('amount'),
                        F.countDistinct('unique_mem_id').alias('these_users')
                        )
                   .join(holidayUsersCount, ['years', 'holiday'])
                   .join(holidayDayCount, ['years', 'holiday'])
                   .withColumn('amount', F.col('amount').cast(DoubleType()))
                   .withColumn('transactions', F.col('transactions').cast(DoubleType()))
                   .withColumn('average_transaction', F.col('amount') / F.col('transactions'))
                   .withColumn('transactions_per_user', (F.col('transactions') / F.col('these_users')))
                   .withColumn('amount_per_user', (F.col('amount') / F.col('these_users')))
                   .withColumn('transactions', F.col('transactions') / F.col('unique_users'))
                   .withColumn('amount', F.col('amount') / F.col('unique_users'))
                  .drop('these_users', 'unique_users', 'days')
                  )

# COMMAND ----------

holiday_metrics.write.mode('overwrite').format('delta').save(holiday_metrics_path)

# COMMAND ----------

aurora_insight_upload(holiday_metrics_path)

# COMMAND ----------

