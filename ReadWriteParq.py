from pyspark.sql import SparkSession


ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

pardf=ssn.read.options(header='true', inferSchema='true').parquet("file:///home/gopalkrishna/file.parquet")

pardf.write.mode("overwrite").parquet("file:///home/gopalkrishna/dirparquet")