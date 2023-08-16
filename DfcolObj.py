from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

ssn=SparkSession.builder.appName("RDD2DF").master("local[*]").getOrCreate()

data=[['James','','Smith jdckjnsdkjncskjdcksjncksjdnckjndjsncksdcnskjc','1991-04-01','M',3000],
  ['Michael','Rose','','2000-05-19','M',4000],
  ['Robert','','Williams','1978-09-05','M',4000],
  ['Maria','Anne','Jones','1967-12-01','F',4000],
  ['Jen','Mary','Brown','1980-02-17','F',-1]]

clm = ["firstname","middlename","lastname","dob","gender","salary"]

dfm=ssn.createDataFrame(data,clm)
ldf=dfm.withColumn("loc",lit("HYD"))

# sdf=dfm.select(col("middlename")).show()
# ndf=dfm.select(dfm.firstname).show()
# msel=ldf.select("firstname","salary","loc").show()
# dfl=ldf.select(ldf.middlename,ldf.dob,ldf.gender).show()
#dfl=ldf.select(col("middlename"),col("gender")).show()
dfl=ldf.select("*").show()
#print(ldf.columns)