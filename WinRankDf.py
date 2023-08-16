from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), ("Michael", "Sales", 4600), ("Robert", "Sales", 4100), ("Maria", "Finance", 3000),("James", "Sales", 3000),("Scott", "Finance", 3300),("Jen", "Finance", 3900),("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000),
              ("Kumar", "Marketing", 2000),              \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100), \
              ("Saif", "Sales", 4100),\
              ("Saif", "Sales", 4100))

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)

Windospec=Window.partitionBy("department").orderBy("salary")


ndf=df.withColumn("RANK",rank().over(Windospec))
ndf.show()
# fdf=ndf.filter(ndf.rno==1)
# rdf=fdf.drop("rno")
# rdf.show()
