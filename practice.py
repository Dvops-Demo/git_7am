from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from operator import add
spark=SparkSession.builder.master("local[*]").appName("practice").getOrCreate()
sc=spark.sparkContext
rd=sc.textFile("prac.txt")
print(rd.collect())
frdd=rd.flatMap(lambda x:x.split(" "))
print(frdd.collect())
mrd=frdd.map(lambda y:(y,1))
print(mrd.collect())
redu=mrd.reduceByKey(add)
print(redu.collect())
