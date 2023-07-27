from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('SparkByExamples.com').master("local[*]").getOrCreate()
data = [("James","M",60000),("Michael","M",70000),("Robert",None,400000),("Maria","F",500000),("Jen","",None)]
columns = ["name","gender","salary"]
df = spark.createDataFrame(data,columns)
ndf=df.withColumn("new_gen",when(df.gender=='M',"MALE").when(df.gender=='F',"FEMALE").when(df.gender.isNull(),"").otherwise(df.gender))
ndf.show()