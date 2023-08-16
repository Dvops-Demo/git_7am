from pyspark.sql import SparkSession
import sys
ssn=SparkSession.builder.appName("JDBC_READER").enableHiveSupport().getOrCreate()
htbl=sys.argv[1]
mtbl=sys.argv[2]
mdb=sys.argv[3]

df=ssn.read.table(htbl)

df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/{}".format(mdb)) \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", mtbl) \
    .option("user", "root") \
    .option("password", "root") \
    .save()

ssn.stop()
