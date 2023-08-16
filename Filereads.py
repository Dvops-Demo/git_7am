from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

####CSV
df=spark.read.options(header='true', inferSchema='true').csv("file:///home/gopalkrishna/file.csv")
df.write.mode("overwrite").csv("file:///home/gopalkrishna/sparkout")
df.write.mode("append").csv("file:///home/gopalkrishna/sparkout")