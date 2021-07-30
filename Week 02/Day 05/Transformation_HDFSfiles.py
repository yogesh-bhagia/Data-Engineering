import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import when
class Pipeline:
        
    def __init__(self):
        self.spark=None
        bureauDF=None
        b_balanceDF=None
        
        
    def create_spark_session(self):
        self.spark = SparkSession.builder\
        .appName("my first spark app")\
        .config("spark.driver.extraClassPath","pipeline/postgresql-42.2.18.jar")\
        .enableHiveSupport().getOrCreate()
        
    def read_hdfs_files(self):
        #____WORKING ON BUREAU.csv____
        self.bureauDF = self.spark.read.csv("/user/csv_data/bureau.csv", header=True, inferSchema=True)
        df1=self.bureauDF.filter(col("CREDIT_ACTIVE") == "Active")
        df3=df1.filter(col("CREDIT_DAY_OVERDUE") == 0)
     
     
        #______TYPE CAST 4 COLUMNS OF BUREAU.csv____
        df4 = df3.withColumn("AMT_CREDIT_SUM",col("AMT_CREDIT_SUM").cast(DecimalType(30, 10))) 
        df5 = df4.withColumn("AMT_CREDIT_SUM_DEBT",col("AMT_CREDIT_SUM_DEBT").cast(DecimalType(30, 10)))
        df6 = df5.withColumn("AMT_CREDIT_SUM_LIMIT",col("AMT_CREDIT_SUM_LIMIT").cast(DecimalType(30, 10)))
        df7 = df6.withColumn("AMT_CREDIT_SUM_OVERDUE",col("AMT_CREDIT_SUM_OVERDUE").cast(DecimalType(30, 10)))     
   

 
        #_____WORKING ON BUREAU_BALANCE.csv______
        self.b_balanceDF = self.spark.read.csv("/user/csv_data/bureau_balance.csv", header=True, inferSchema=True)
        
        #_____APPLYING CASE STAEMENTS ON 'STATUS' COLUMN OF BUREAU_BALANCE.csv_____
        df8 = self.b_balanceDF.withColumn("STATUS",when(self.b_balanceDF.STATUS == 'C', 'closed').when(self.b_balan
ceDF.STATUS == 'X', 'status unknown').when(self.b_balanceDF.STATUS == 0, 'no DPD').when(self.b_balanceDF.STATUS == 
1, ' DPD 1-30').when(self.b_balanceDF.STATUS == 2, 'DPD 31-60').when(self.b_balanceDF.STATUS == 3, 'DPD 61-90').whe
n(self.b_balanceDF.STATUS == 4, 'DPD 91-120').when(self.b_balanceDF.STATUS == 5, 'DPD 120+').otherwise('sold or wri
tten off'))
        #_____JOINING (Modified BUREAU with Modified BUREAU_BALANCE)_______
        #j1=df8.join(df7,df8.SK_ID_BUREAU==df7.SK_ID_BUREAU, 'inner')
        J1=df8.join(df7,"SK_ID_BUREAU")
        J1.show(n=10)
        
        #____STORING DATAFRAME ON NEW csv FILE____
        //J1.write.format("csv").save("/user/csv_data/balance_bureau")
        J1.coalesce(1).write.mode('overwrite').option('header','true').csv('/user/csv_data/balance_bureau.csv')
if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.read_hdfs_files() 
