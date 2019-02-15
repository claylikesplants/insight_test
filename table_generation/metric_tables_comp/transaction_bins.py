# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

cohortTable = spark.read.format("delta").load(dbutils.widgets.get("cohortTable_table_path"))
transaction_bins_path = dbutils.widgets.get("transaction_bins_table_path")

# cohortTable = spark.table('insight.grocery_outlet_cohorttable')
# transaction_bins_path = 'insight.grocery_outlet_transaction_bins'
bins = [5, 15, 35, 55]

# COMMAND ----------

# Create a table which breaks down users by the amount of times they
# transacted with a given company
w = Window.partitionBy('cohortBin').rangeBetween(-sys.maxsize, sys.maxsize)
v = Window.partitionBy('clean_company').rangeBetween(-sys.maxsize, sys.maxsize)
# Define bin dictionary
bin_names = {1: '1 - ' + str(bins[0] - 1) + ' Transactions',
             2: str(bins[0])+ ' - ' + str(bins[1] - 1) + ' Transactions',
             3: str(bins[1]) + ' - ' + str(bins[2] - 1) + ' Transactions',
             4: str(bins[2]) + ' - ' + str(bins[3] - 1) + ' Transactions',
             5: '> ' + str(bins[3]) + ' Transactions'
            }
transaction_bins = (cohortTable
        .select('Date', 'unique_mem_id', 'clean_company')
        .groupBy('Date', 'unique_mem_id', 'clean_company')
        .agg(
            (F.rank().over(Window.partitionBy('unique_mem_id').orderBy('Date'))).alias('pymt_num')
        )
        .groupBy('unique_mem_id', 'clean_company')
        .agg(
            F.max('pymt_num').alias('cohort'))
        # Assign users to bins
        .withColumn('cohortBin', F.when(F.col('cohort') < bins[0], bin_names[1])
                               .when((F.col('cohort') >= bins[0]) & (F.col('cohort') < bins[1]), bin_names[2])
                               .when((F.col('cohort') >= bins[1]) & (F.col('cohort') < bins[2]), bin_names[3])
                               .when((F.col('cohort') >= bins[2]) & (F.col('cohort') < bins[3]), bin_names[4])
                               .when(F.col('cohort') >= bins[3], bin_names[5])
                   )
        .groupBy(F.col('cohortBin'), F.col('clean_company'))
        .agg(F.count('cohortBin').alias('cohortCount'))
        .withColumn('cohortSumBin', F.sum('cohortCount').over(w))
        .withColumn('cohortSumCompany', F.sum('cohortCount').over(v))
        # Find probability a given type of user will be at a given company
        .withColumn('retentionbyTransactionBin', F.col('cohortCount') / F.col('cohortSumBin'))
        # Find probability a a given company will have a given type of user
        .withColumn('retentionbyTransactionCompany', F.col('cohortCount') / F.col('cohortSumCompany'))
        .withColumn('cohortBinOrder', F.when(F.col('cohortBin') == bin_names[1], 1)
                                 .when(F.col('cohortBin') == bin_names[2], 2)
                                 .when(F.col('cohortBin') == bin_names[3], 3)
                                 .when(F.col('cohortBin') == bin_names[4], 4)
                                 .when(F.col('cohortBin') == bin_names[5], 5)
                     )
        .drop('cohortCount', 'cohortSumBin', 'cohortSumCompany')
       )

# COMMAND ----------

transaction_bins.write.mode('overwrite').format('delta').save(transaction_bins_path)

# COMMAND ----------

aurora_insight_upload(transaction_bins_path)

# COMMAND ----------

