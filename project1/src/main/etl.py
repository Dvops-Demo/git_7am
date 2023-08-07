from pyspark.sql import SparkSession
import sys


ssn=SparkSession.builder.appName("PROJECT_1").enableHiveSupport().getOrCreate()

# fl_name=sys.argv[1]
#
# fo=open(fl_name,"r")
#
# sql_qry=fo.read()

# print(">>>>>>>>>>>SQL QUERY BELOW<<<<<<<<<<<<<<<\n")
# print(sql_qry)
# print(">>>>>>>>>>>SQL QUERY<<<<<<<<<<<<<<<\n")

df=ssn.sql("select e.id,e.name,e.age,d.dname from mydb.emp e join mydb.dept d on e.id=d.eid")

df.write.mode("overwrite").saveAsTable("mydb.FTB")

ssn.stop()