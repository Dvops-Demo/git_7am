from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import col,lit
spark=SparkSession.builder.appName("Processing").enableHiveSupport().getOrCreate()
df=spark.sql("select d.pname,d.age,d.pid,r.score from hdemo d join hreport r on d.pid=r.pid")
ndf=df.withColumn("Diabetic",when(df.score < 130,"Normalrange")\
                  .when(df.score.between(130,150),"Prediabetic")\
                  .when(df.score > 150,"Diabetic").otherwise("Invalid"))

ndf.write.mode("overwrite").saveAsTable("siriprac.report_tb")
spark.stop()