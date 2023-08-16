from pyspark.sql import SparkSession
from operator import add
from operator import mul
ssn=SparkSession.builder.master("local[*]").appName("RDD_ACTIONS").getOrCreate()
scon=ssn.sparkContext


#rdd=scon.textFile("rdd_action.txt")
#par_rdd=scon.parallelize([1,2,3,4])
fl_par=scon.parallelize(["apple","mango","ball"])
# print("Collect Action result")
# print(rdd.collect())
# print("\n")
# print("Count Action result\n")
# print(rdd.count())
# print("\n")
# print("first Action result")
# print(rdd.first())
# print("\n")
# print("MAX Action result")
# print(par_rdd.max())
# print("\n")
# print("reduce Action result")
# print(par_rdd.reduce(mul))
# print("\n")

print("filter trn result")
flr_res=fl_par.filter(lambda x:x=="apple") #[[101,RAM,5000][2], [102,SAM,6000], [103,KIM,4000], [104,Jim,8000]]
print(flr_res.collect())
print("\n")


