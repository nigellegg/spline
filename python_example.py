from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as sf
import logging
sc = SparkContext()

spark = SparkSession \
    .builder \
    .master("local[1]")\
    .appName("SparkByExamples.com")\
    .getOrCreate()

empsDF = spark.read \
    .option("header", "true") \
    .option("inferschema", "true") \
    .csv("emp_data.csv") 
#log.warn('empsDF created')
empsDF1 = empsDF.withColumnRenamed('name', 'Name')
empsDF1.show()

deptsDF = spark.read \
    .option("header", "true") \
    .option("inferschema", "true") \
    .csv("dept.csv")
#log.warn('deptsDF created')


resultsDF = empsDF1.join(deptsDF, empsDF1.dept_id==deptsDF.dept_id1, "left_outer")
resultsDF.write.csv( 'results.csv', header=True)
xdf = empsDF.groupBy('manager_id')
ydf = xdf.agg(sf.sum('salary').alias('total_salary'))
ydf.show()
#log.warn('dfs joined')
ydf.coalesce(1).write.csv( 'agg.csv', header=True)
