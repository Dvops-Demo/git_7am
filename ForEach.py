import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

scon=spark.sparkContext

#accum=spark.sparkContext.accumulator(0)
rdd=scon.parallelize([1,2,3,4,5])
nrdd=rdd.foreach(lambda x:print(x+1))
# frdd=nrdd.collect()
# print(frdd)