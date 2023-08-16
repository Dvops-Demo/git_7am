from pyspark.sql import SparkSession

ssn=SparkSession.builder.appName("RDD2DF").master("local[*]").getOrCreate()

scon=ssn.sparkContext

rdd=scon.parallelize([("Finance",10),("Marketing",20),("Sales",30),("IT",40)])

clm=["Dept","Did"]

df=rdd.toDF(clm)

df.show()
df.printSchema()

#print(rdd.collect())