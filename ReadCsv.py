from pyspark.sql import SparkSession

ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

csdf=ssn.read.options(header='true', inferSchema='true').csv("C:/Users/ashok/PycharmProjects/git_7am_prac/emp.csv")

csdf.write.mode("overwrite").csv("C:/Users/ashok/PycharmProjects/git_7am_prac/dircsv")