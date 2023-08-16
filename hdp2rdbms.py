# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "mysql-connector-java-6.0.6.jar") \
           .getOrCreate()

df=spark.sql("select * from processed_data_table")

df.write \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .mode("overwrite") \
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;") \
  .option("dbtable", "employee") \
  .option("user", "replace_user_name") \
  .option("password", "replace_password") \
  .save()