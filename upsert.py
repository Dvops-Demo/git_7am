from pyspark.sql import SparkSession

from pyspark.sql.functions import *

ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

new_df=ssn.read.options(header="true",inferSchema="true").csv("new.csv")

old_df=ssn.read.options(header="true",inferSchema="true").csv("old.csv")


jdf=new_df.join(old_df,new_df.eid==old_df.eid,"fullouter").select(when(new_df.eid.isNull(),old_df.eid).otherwise(new_df.eid).alias("eid")
                                                                  ,when(new_df.ename.isNull(),old_df.ename).otherwise(new_df.ename).alias("ename")
                                                                  ,when(new_df.sal.isNull(),old_df.sal).otherwise(new_df.sal).alias("sal")
                                                                  ,when(new_df.loc.isNull(),old_df.loc).otherwise(new_df.loc).alias("loc"))


#,when(new_df("ename").isNull(),old_df("ename")).otherwise(new_df("ename")),when(new_df("sal").isNull(),old_df("sal")).otherwise(new_df("sal")),when(new_df("sal").isNull(),old_df("sal")).otherwise(new_df("sal")))


