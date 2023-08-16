from pyspark.sql import SparkSession
from pyspark.sql.functions import *

ssn=SparkSession.builder.appName("RDD2DF").master("local[*]").getOrCreate()

data=[['James','','Smith jdckjnsdkjncskjdcksjncksjdnckjndjsncksdcnskjc','1991-04-01','M',3000,'NY'],
  ['Michael','Rose','','2000-05-19','M',4000,'NY'],
  ['Robert','','Williams','1978-09-05','M',4000,'IL'],
  ['Maria','Anne','Jones','1967-12-01','F',4000,'IL'],
['Maria','Anne','Jones','1967-12-01','F',4000,'CA'],
  ['Jen','Mary','Brown','1980-02-17','F',-1,'CA'],
  ['Jen','Mary','Brown','1980-02-17','F',-1,'FL']]

clm = ["firstname","middlename","lastname","dob","gender","salary","loc"]

dfm=ssn.createDataFrame(data,clm)
dfm.groupBy("loc").sum("salary").show()