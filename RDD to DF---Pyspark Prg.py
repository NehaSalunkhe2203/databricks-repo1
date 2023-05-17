# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark import SparkConf,SparkContext

# COMMAND ----------

confobj1=SparkConf().setMaster("local[1]").setAppName("todf")

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=confobj1)

# COMMAND ----------

r1=sc.textFile('dbfs:/FileStore/shared_uploads/salunkhen442@gmail.com/custorder.csv')

# COMMAND ----------

r5=r1.collect()

# COMMAND ----------

r2=[['neha','kirti','nidhi']]

# COMMAND ----------

r3=sc.parallelize(r2)

# COMMAND ----------

df1=r3.collect()

# COMMAND ----------

df2=r3.toDF().show()

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
spark= SparkSession.builder.master("local[1]").appName('join').getOrCreate()
custorders=spark.read.option('header','true').option('inferschema','true').csv('dbfs:/FileStore/shared_uploads/salunkhen442@gmail.com/amt.csv')


# COMMAND ----------

#create DF to rdd

rdd1=custorders.rdd

# COMMAND ----------

rdd1.collect()

# COMMAND ----------


