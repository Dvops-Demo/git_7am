from pyspark.sql import SparkSession

from os.path import abspath

warehouse_loc=abs("spark-warehouse")

ssn=SparkSession.builder.master("local[*]")\
    .appName("spark-hive-integration")\
    .config("spark.sql.warehouse.dir",warehouse_loc) \
    .enableHiveSupport() \
    .getOrCreate()


df=ssn.read.table("mydb.emp")
df.show()

df1=ssn.sql("select * from mydb.emp")
df1.show()

