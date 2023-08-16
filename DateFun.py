from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
currentDatedf=df.withColumn("current_date",current_date())
datefr=df.withColumn("newdate",date_format(df.input,"dd/MM/yyyy"))
datedif=df.withColumn("currentdate",current_date())\
    .withColumn("diffDays",datediff(current_date(),df.input))
monthdiff=df.withColumn("currentdate",current_date()) \
    .withColumn("monthsdiff",months_between(current_date(),df.input))

addmon=df.withColumn("futDate",add_months(df.input,3))

addmon.show()
