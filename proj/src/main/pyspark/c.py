from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
spark=SparkSession.builder.appName("processing").enableHiveSupport().getOrCreate()
df=spark.sql("select * from siriprac.upcan")
ndf=df.withColumn("Risk",when(df.nscore.between(20.4,30.5),"less").when(df.nscore > 30.5, "High").otherwise("Normal"))
ndf.write.mode("overwrite").saveAsTable("siriprac.hlthcon")
spark.show()