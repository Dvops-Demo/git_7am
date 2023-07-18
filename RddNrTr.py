from pyspark.sql import SparkSession
from operator import add

spsn=SparkSession.builder.master("local[*]").appName("RDD_NARROW_TRAN").getOrCreate()

scn=spsn.sparkContext

rdd=scn.textFile("test.txt") #base rdd
mp_rdd=rdd.map(lambda x: x.split(" ")) #map rdd
fmp_rdd=mp_rdd.flatMap(lambda x:x) #flat map rdd

kvrdd=fmp_rdd.map(lambda x:(x,1))

rdby=kvrdd.reduceByKey(add)

base_rdd_res=rdd.collect() #result

map_rdd_res=mp_rdd.collect()

flt_map_rdd_res=fmp_rdd.collect()

kv_rdd_res=kvrdd.collect()
reduce_by_res=rdby.collect()

print("=====base_rdd_res====\n")
print(base_rdd_res)
print("=====map_rdd_res====\n")
print(map_rdd_res)
print("=====flt_map_rdd_res====\n")
print(flt_map_rdd_res)
print("=====kv_rdd_res====\n")
print(kv_rdd_res)
print("=====reduce_by_res====\n")
print(reduce_by_res)

grpby=kvrdd.groupByKey().mapValues(len).collect()
print(grpby)
