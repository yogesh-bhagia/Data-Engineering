# LOADING FILE IN MySQL CREATED TABLE (with Proper Schema)
#=========================================================


# Import Statements
#--------------------------------------------------
import pandas as pd
import pymysql
from sqlalchemy import create_engine


# Reading File
#-------------------------------------------------
df_A = pd.read_csv("/home/ybhagia2005/csv_files/application_test.csv")
df_B = pd.read_csv("/home/ybhagia2005/csv_files/application_train.csv")
df_C = pd.read_csv("/home/ybhagia2005/csv_files/burreau.csv")
df_D = pd.read_csv("/home/ybhagia2005/csv_files/burreau_balance.csv")



# Just for Quering Name and Number of Columns
#-------------------------------------------------
df_A.head()
df_A.columns


# Creating Connection
#------------------------------------------------
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="password",
                               db="demo"))


# String is stored in engine variable...  
#-----------------------------------------------
df_A.to_sql(con=engine, name='application_test_table', if_exists='replace')


