from pyspark.sql import SparkSession
from pyspark.sql.functions import *

ssn=SparkSession.builder.appName("RDD2DF").master("local[*]").getOrCreate()

data=[['James','','Smith jdckjnsdkjncskjdcksjncksjdnckjndjsncksdcnskjc','1991-04-01','M',3000],
  ['Michael','Rose','','2000-05-19','M',4000],
  ['Robert','','Williams','1978-09-05','M',4000],
  ['Maria','Anne','Jones','1967-12-01','F',4000],
['Maria','Anne','Jones','1967-12-01','F',4000],
  ['Jen','Mary','Brown','1980-02-17','F',-1],
  ['Jen','Mary','Brown','1980-02-17','F',-1]]

clm = ["firstname","middlename","lastname","dob","gender","salary"]

dfm=ssn.createDataFrame(data,clm)
dup_drop=dfm.dropDuplicates()
drdf=dup_drop.drop("salary")
drdf.show()