from pyspark.sql import SparkSession
from pyspark.sql.functions import *


ssn=SparkSession.builder.appName("ETL_APP").getOrCreate()
df=ssn.sql("select * from hivetable")
df.write.parquet("hive ware house location")