from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from datetime import datetime

ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

dmgrp_df = ssn.read.options(header="true", inferSchema="true").csv("mem_dmgrp.csv")

lab_df=ssn.read.options(header="true",inferSchema="true").csv("lab_metrics.csv")
now=datetime.now()
yr= now.strftime("%y")
mn= now.strftime("%m") #09
myr=mn + yr

pmn=mn-1
pym=pmn + yr

prev_month_df=ssn.sql("select * from rpt_dbc_metric where yrm={}".format(pym))

jdf=dmgrp_df.join(lab_df,dmgrp_df.pid==lab_df.pid,"inner").select(dmgrp_df.pid,dmgrp_df.pname,dmgrp_df.age,dmgrp_df.gender,lab_df.diag,lab_df.score)\
    .withColumn("status",when(lab_df.score>120,"High").otherwise("Low"))\
    .withColumn("progress",when(lab_df.score > prev_month_df.score,"Red").otherwise("Green")) \
    .withColumn("yrm",lit(myr))

ldf=jdf.write.mode("append").saveAsTable("rpt_dbc_metric")






#,when(new_df("ename").isNull(),old_df("ename")).otherwise(new_df("ename")),when(new_df("sal").isNull(),old_df("sal")).otherwise(new_df("sal")),when(new_df("sal").isNull(),old_df("sal")).otherwise(new_df("sal")))


