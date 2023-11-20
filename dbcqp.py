from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from datetime import datetime

ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

dmgrp_df = ssn.sql("select * from mydb.mem_dmg")

lab_df=ssn.sql("select * from mydb.lab_metric")

now=datetime.now()
yr= now.strftime("%y")
mn= now.strftime("%m") #09
myr=mn + yr

pmn=mn-1
pym=pmn + yr



current_data=dmgrp_df.join(lab_df,dmgrp_df.pid==lab_df.pid,"inner").select(dmgrp_df.pid,dmgrp_df.pname,dmgrp_df.age,dmgrp_df.gender,lab_df.diag,lab_df.score) \

current_data.write.saveAsTable("mydb.rpt_dbc_metric")

prev_month_df=ssn.sql("select * from rpt_dbc_metric where yrm={}".format(pym))

cur_prev_df=current_data.join(prev_month_df,current_data.pid==prev_month_df.pid,"leftouter").select(current_data.pid,current_data.pname,current_data.age,current_data.gender,current_data.diag,current_data.score)\
    .withColumn("status",when(current_data.score>120,"High").otherwise("Low"))\
    .withColumn("progress",when(current_data.score > prev_month_df.score,"Red").otherwise("Green")) \
    .withColumn("yrm",lit(myr))

ldf=cur_prev_df.write.mode("append").saveAsTable("mydb.rpt_dbc_metric")


