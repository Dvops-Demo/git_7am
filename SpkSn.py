from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("RDD_CLASS").master("local[*]").getOrCreate()

sc=spark.sparkContext
#sc.setSystemProperty("hadoop.home.dir","C:/Users/ashok/Downloads/winutils-master/winutils-master/hadoop-2.6.0")

rdd=sc.parallelize(["hellow","ola","fella","oorah","rock","roll"])

#rdd.foreach(lambda x:print(x))
print(rdd.count())