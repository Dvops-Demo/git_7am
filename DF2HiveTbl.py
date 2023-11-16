from pyspark.sql import SparkSession

from os.path import abspath

warehouse_loc=abs("spark-warehouse")

ssn=SparkSession.builder.master("local[*]")\
    .appName("spark-hive-integration")\
    .config("spark.sql.warehouse.dir",warehouse_loc) \
    .enableHiveSupport() \
    .getOrCreate()

dfhyd=ssn.sql("select * from db.emp where loc='hyd'")



dfhyd.fillna("UNK")
dfhyd.write.mode("overwrite/append").saveAsTable("db.hydemp")



