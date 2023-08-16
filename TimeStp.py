from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()

data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]

df2=spark.createDataFrame(data,["id","input"])

Tsf=df2.withColumn("input_clean",to_timestamp(df2.input,"MM-dd-yyyy HH mm ss SSS"))\
    .withColumn("current_stamp",current_timestamp())

hdf=Tsf.withColumn("hour_input_clean",hour(Tsf.input_clean))
mdf=hdf.withColumn("min_input_clean",minute(hdf.input_clean))

mdf.show(truncate=False)