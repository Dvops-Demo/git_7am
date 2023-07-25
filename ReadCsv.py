from pyspark.sql import SparkSession

ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

csdf=ssn.read.options(header='true', inferSchema='true').csv("file:///home/gopalkrishna/file.csv")

csdf.write.mode("overwrite").csv("file:///home/gopalkrishna/dircsv")