from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

ssn=SparkSession.builder.appName("RDD2DF").master("local[*]").getOrCreate()

scon=ssn.sparkContext

rdd=scon.parallelize([("Finance",10),("Marketing",20),("Sales",30),("IT",40)])

col=StructType([StructField("dept",StringType(),True),StructField("did",StringType(),True)])

df=ssn.createDataFrame(rdd,col)
df.show()
df.printSchema()