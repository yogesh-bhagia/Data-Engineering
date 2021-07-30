import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when
from pyspark import SparkContext, HiveContext
class Pipeline:
        
    def __init__(self):
        self.spark=None
        DF1_cre_card_bal=None
        
        
        
    def create_spark_session(self):
        self.spark = SparkSession.builder\
        .appName("my first spark app")\
        .config("spark.driver.extraClassPath","pipeline/postgresql-42.2.18.jar")\
        .enableHiveSupport().getOrCreate()
        
    def read_from_hive(self):
        
        #_________CREDIT_CARD_BALANCE___________
        self.DF1_cre_card_bal=self.spark.sql("select * from credit_card_balance where NAME_CONTRACT_STATUS != 'Refused' ")
        #self.DF1_cre_card_bal=self.sqlContext.sql("select * from credit_card_balance where NAME_CONTRACT_STATUS != 'Refused' ")
        self.DF1_cre_card_bal.limit(5).show()
        
        #________TYPE CAST_________
        df1 = self.DF1_cre_card_bal.withColumn("months_balance",col("months_balance").cast(DecimalType(30, 10)))
        df2 = df1.withColumn("AMT_BALANCE",col("AMT_BALANCE").cast(DecimalType(30, 10)))
        df3 = df2.withColumn("AMT_CREDIT_LIMIT_ACTUAL",col("AMT_CREDIT_LIMIT_ACTUAL").cast(DecimalType(30, 10)))
        df4 = df3.withColumn("AMT_DRAWINGS_ATM_CURRENT",col("AMT_DRAWINGS_ATM_CURRENT").cast(DecimalType(30, 10)))
        df5 = df4.withColumn("AMT_DRAWINGS_CURRENT",col("AMT_DRAWINGS_CURRENT").cast(DecimalType(30, 10)))
        df6 = df5.withColumn("AMT_DRAWINGS_OTHER_CURRENT",col("AMT_DRAWINGS_OTHER_CURRENT").cast(DecimalType(30, 10)))
        df7 = df6.withColumn("AMT_DRAWINGS_POS_CURRENT",col("AMT_DRAWINGS_POS_CURRENT").cast(DecimalType(30, 10)))
        df8 = df7.withColumn("AMT_INST_MIN_REGULARITY",col("AMT_INST_MIN_REGULARITY").cast(DecimalType(30, 10)))
        df9 = df8.withColumn("AMT_PAYMENT_CURRENT",col("AMT_PAYMENT_CURRENT").cast(DecimalType(30, 10)))
        df10 = df9.withColumn("AMT_PAYMENT_TOTAL_CURRENT",col("AMT_PAYMENT_TOTAL_CURRENT").cast(DecimalType(30, 10)))
        df11 = df10.withColumn("AMT_RECEIVABLE_PRINCIPAL",col("AMT_RECEIVABLE_PRINCIPAL").cast(DecimalType(30, 10)))
        df12 = df11.withColumn("AMT_RECIVABLE",col("AMT_RECIVABLE").cast(DecimalType(30, 10)))
        df13 = df12.withColumn("AMT_TOTAL_RECEIVABLE",col("AMT_TOTAL_RECEIVABLE").cast(DecimalType(30, 10)))

                
        df13.printSchema()
        #df13=sqlContext.sql("select * from credit_card_balance where NAME_CONTRACT_STATUS != 'Refused' ")
        
        
        
        #__________INSTALLMENTS_PAYMENTS_____________
        self.DF2_ins_pay=self.spark.sql("select * from installments_payments")
        #self.DF2_ins_pay=self.sqlContext.sql("select * from installments_payments")
        self.DF2_ins_pay.limit(5).show()
        #________TYPE CAST_________
        df011 = self.DF2_ins_pay.withColumn("NUM_INSTALMENT_NUMBER",col("NUM_INSTALMENT_NUMBER").cast(IntegerType()))
        df012 = df011.withColumn("AMT_INSTALMENT",col("AMT_INSTALMENT").cast(DecimalType(30, 10)))
        df013 = df012.withColumn("AMT_PAYMENT",col("AMT_PAYMENT").cast(DecimalType(30, 10)))
        df013.printSchema() 
        #df013=sqlContext.sql("select * from installments_payments")
        
        #__________POS_CASH_BALANCE_____________
        self.DF3_P_C_Bal=self.spark.sql("select * from POS_CASH_balance")
        self.DF3_P_C_Bal.limit(5).show()
        self.DF3_P_C_Bal.printSchema()
        
        
        #_________JOIN B/W CREDIT_CARD_BALANCE and INSTALLMENTS_PAYMENTS________
        J1=df13.join(df013,df13.SK_ID_CURR == df013.SK_ID_CURR, 'inner')
           
if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.read_from_hive()
