from pyspark.sql import SparkSession

spk=SparkSession.builder.master("local[*]").appName("RDD_TXTFILE").getOrCreate()

spcon=spk.sparkContext

rdd_txtf=spcon.textFile("emp.csv")


#print(rdd_txtf.collect())
print(rdd_txtf)
