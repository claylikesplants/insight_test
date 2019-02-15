# Databricks notebook source
# MAGIC %run "../helper_functions"

# COMMAND ----------

# state_to_region_path = dbutils.widgets.get("state_to_region_table_path")
state_to_region_path = 'dbfs:/mnt/mscience-dev/databases/insight/state_to_region'

# COMMAND ----------

state_abr_to_region = {
  "AL": "South", 
  "AK": "West", 
  "AZ": "West", 
  "AR": "South", 
  "CA": "West", 
  "CO": "West", 
  "CT": "Northeast", 
  "DE": "South", 
  "DC": "South", 
  "FL": "South", 
  "GA": "South", 
  "HI": "West", 
  "ID": "West", 
  "IL": "Midwest", 
  "IN": "Midwest", 
  "IA": "Midwest", 
  "KS": "Midwest", 
  "KY": "South", 
  "LA": "South", 
  "ME": "Northeast", 
  "MD": "South", 
  "MA": "Northeast", 
  "MI": "Midwest", 
  "MN": "Midwest", 
  "MS": "South", 
  "MO": "Midwest", 
  "MT": "West", 
  "NE": "Midwest", 
  "NV": "West", 
  "NH": "Northeast", 
  "NJ": "Northeast", 
  "NM": "West", 
  "NY": "Northeast", 
  "NC": "South", 
  "ND": "Midwest", 
  "OH": "Midwest", 
  "OK": "South", 
  "OR": "West", 
  "PA": "Northeast", 
  "PR": "South", 
  "RI": "Northeast", 
  "SC": "South", 
  "SD": "Midwest", 
  "TN": "South", 
  "TX": "South", 
  "UT": "West", 
  "VT": "Northeast", 
  "VA": "South", 
  "WA": "West", 
  "WV": "South", 
  "WI": "Midwest", 
  "WY": "West"
}

# COMMAND ----------

# Convert dictionary to df
state_abr_to_region = sc.parallelize(state_abr_to_region.items()).toDF(['state_abr', 'region'])

# COMMAND ----------

state_abr_to_name = spark.read.format('delta').load('dbfs:/mnt/mscience-dev/databases/insight/state_abr_to_name')
state_to_region = (state_abr_to_region
                   .join(state_abr_to_name, ['state_abr'])
                   .drop('state_abr')
                  )

# COMMAND ----------

state_to_region.write.mode('overwrite').format('delta').save(state_to_region_path)