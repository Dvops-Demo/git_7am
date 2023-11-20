from  pyspark.sql import SparkSession
from operator import add

ssn=SparkSession.builder.master("local[*]").appName("Word COUNT").getOrCreate()
spc=ssn.sparkContext

rd=spc.textFile("prac.txt",use_unicode=False)
flt_mp=rd.flatMap(lambda y:y.split(" "))
mp_rd=flt_mp.map(lambda y:(y,1))
redby=mp_rd.reduceByKey(add)
filter_rd=flt_mp.filter(lambda x:len(x)==5)

print(rd.collect())
print(flt_mp.collect())
print(mp_rd.collect())
print(redby.collect())
srt_res=redby.sortByKey(ascending="false")
print(srt_res.collect())
#print(filter_rd.take())