# LOADING FILE IN MySQL CREATED TABLE (with Proper Schema)
#=========================================================


# Import Statements
#--------------------------------------------------
import pandas as pd
import pymysql
from sqlalchemy import create_engine


# Reading File
#-------------------------------------------------
A = pd.read_csv("/home/ybhagia2005/csv_files/application_test.csv")


# Just for Quering Name and Number of Columns
#-------------------------------------------------
A.head()
A.columns


# Creating Connection
#------------------------------------------------
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="password",
                               db="demo"))


# String is stored in engine variable...  
#-----------------------------------------------
A.to_sql(con=engine, name='application_test_table', if_exists='replace')


