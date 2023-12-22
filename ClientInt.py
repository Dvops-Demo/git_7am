from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class ClientInt:

    def readparq(self):
        sn = SparkSession.builder.appName("lkdsmc").getOrCreate()
        df=sn.read.parquet("s3://freelight/trash/dump/sample.parquet")
        df.registerTempTable("emp")

    def trns(self,df,sn):
        trns_df=sn.sql("sleect * from emp where loc='hyd'")

    def jsontos3(self,df):
        df.write.json("s3://freelight/trash/jsonfilepath")

        #just a commit from pycharm
#another commit
if __name__ == '__main__':
    ob=ClientInt()
    ob.readparq()
    ob.trns()
    ob.jsontos3()






