from pyspark.sql import SparkSession


ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

pardf=ssn.read.options(header='true', inferSchema='true').json("file:///home/gopalkrishna/file.json")

pardf.write.mode("overwrite").json("file:///home/gopalkrishna/dirjson")