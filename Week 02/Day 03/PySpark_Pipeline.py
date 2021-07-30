

import sys
from pyspark.sql import SparkSession

class Pipeline:   
    def __init__(self):
        self.spark=None
        hd1_bureauDF=None
        hd2_application_testDF=None
        hd3_application_trainDF=None
        hd4_bureau_balanceDF=None
        hi5_credit_card_balanceDF=None
        hi6_installments_paymentsDF=None
        hi7_pos_cash_balanceDF=None
        hi8_previous_applicationDF=None
        
        
    def create_spark_session(self):
        self.spark = SparkSession.builder\
        .appName("my first spark app")\
        .config("spark.driver.extraClassPath","pipeline/postgresql-42.2.18.jar")\
        .enableHiveSupport().getOrCreate()
        
    def read_hdfs_files(self):
        try:
            self.hd1_bureauDF = self.spark.read.csv("/user/csv_data/bureau.csv", header=True, inferSchema=True)
            self.hd2_application_testDF = self.spark.read.csv("/user/csv_data/application_test.csv", header=True, inferSchema=True)
            self.hd3_application_trainDF = self.spark.read.csv("/user/csv_data/application_train.csv", header=True, inferSchema=True)
            self.hd4_bureau_balanceDF = self.spark.read.csv("/user/csv_data/bureau_balance.csv", header=True, inferSchema=True)
        except exception as e:
            print("File Not Found", e)

    def read_hive_table(self):
        try:
            self.hi5_credit_card_balanceDF = self.spark.sql("select * from credit_card_balance")
            self.hi6_installments_paymentsDF = self.spark.sql("select * from installments_payments")
            self.hi7_pos_cash_balanceDF = self.spark.sql("select * from pos_cash_balance")
            self.hi8_previous_applicationDF = self.spark.sql("select * from previous_application")
        except exception as e:
            print("File Not Found", e)
            
    def joins(self):
        try:
        
            #------------------------JOIN B/W HDFS FILE and HDFS FILE------------------------#
            df1=self.hd2_application_testDF.join(self.hd1_bureauDF, self.hd2_application_testDF.SK_ID_CURR==self.hd1_bureauDF.SK_ID_CURR, 'inner')
            print(f'{df1.count()}')
            df2=self.hd4_bureau_balanceDF.join(self.hd1_bureauDF, self.hd4_bureau_balanceDF.SK_ID_BUREAU==self.hd1_bureauDF.SK_ID_BUREAU, 'inner')
            print(f'{df2.count()}')
         
            #-------------------------JOIN B/W HIVE TABLE and HIVE TABLE-----------------------#     
            df3=self.hi8_previous_applicationDF.join(self.hi7_pos_cash_balanceDF, self.hi8_previous_applicationDF.SK_ID_PREV==self.hi7_pos_cash_balanceDF.SK_ID_PREV, 'inner')
            print(f'{df3.count()}')
        
            #---------------------------JOIN B/W HDFS FILE and HIVE TABLE-----------------------#
            df4=self.hi8_previous_applicationDF.join(self.hd2_application_testDF, self.hi8_previous_applicationDF.SK_ID_CURR==self.hd2_application_testDF.SK_ID_CURR, 'inner')
            print(f'{df4.count()}')
            df5=self.hd2_application_testDF.join(self.hi7_pos_cash_balanceDF, self.hd2_application_testDF.SK_ID_CURR==self.hi7_pos_cash_balanceDF.SK_ID_CURR, 'inner')
            print(f'{df5.count()}')
        
        except exception as e:
            print("Something went wrong", e)
            
if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.read_hdfs_files()
    pipeline.read_hive_table()
    pipeline.joins()
    

    
