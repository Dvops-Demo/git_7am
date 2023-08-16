from pyspark.sql import SparkSession

ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

csdf=ssn.read.options(header='true', inferSchema='true').csv("filldata.csv")

csdf.show()
csdf.fillna("UNK").fillna(0).show()
