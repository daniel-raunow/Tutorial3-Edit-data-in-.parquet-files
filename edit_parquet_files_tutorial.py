# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.house_price
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE default.house_price
# MAGIC SET furnishingstatus = 'empty'
# MAGIC WHERE furnishingstatus = 'unfurnished'

# COMMAND ----------

from pyspark.sql.functions import lit

#Create backup
dbutils.fs.mv('/FileStore/tables/house_price', '/FileStore/backup/', recurse = True)

#Read data to data frame
house_price_df = spark.read.parquet('/FileStore/backup/*')

#Filter out rows to edit
house_price_df_toedit = house_price_df.where(house_price_df.furnishingstatus == 'unfurnished')

#Filter out rows to keep
house_price_df_to_keep = house_price_df.where(house_price_df.furnishingstatus != 'unfurnished')

#Edit rows
house_price_df_toedit = house_price_df_toedit.withColumn('furnishingstatus', lit('empty'))

#Recreate dataframe with edited rows
house_price_df = house_price_df_toedit.union(house_price_df_to_keep)

#Write dataframe back to data lake
house_price_df.write.parquet('/FileStore/tables/house_price', mode = 'overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.house_price
# MAGIC WHERE furnishingstatus = 'empty'
# MAGIC LIMIT 100

# COMMAND ----------


