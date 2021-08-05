import json
import countValidation 
import pandas as pd

with open("config.json") as f:
    jsonData = json.load(f)
    csvName = jsonData["csvName"]
    tname = csvName.split(".")[0]
    dbName = jsonData["hiveDBname"]
    csvPath = jsonData['hdfs_csv_location']+jsonData['csvName']

def createHiveTable():
    # df_csv = pd.read_csv('bureau_balance.csv')    
    # columnNames = list(df_csv.columns)
    columnNames = getColumnName(csvPath)

    hiveCmd = "create external table " + tname + " ( "
    rawStr = ') row format delimited fields terminated by ","'

    lastElem = columnNames[-1] + " string ,"
    replaceElem = columnNames[-1] + " string "

    for i in columnNames:
        hiveCmd = hiveCmd + i + " string ,"

    newHiveCmd = hiveCmd.replace(lastElem, replaceElem)
    finalCommnd = newHiveCmd + rawStr

    try:
        with open("hive_Tbl_create.sql", "w") as fp:
            fp.write("use " + dbName + ";\n")
            fp.write("drop table if exists " + tname + ";\n")
            fp.write(finalCommnd + ";\n")
            fp.write("LOAD DATA  INPATH "+"\'"+csvPath+"\'"+" into table "+tname+';')
            
        print("HIVE SQL FILE CREATED ......")
    except Exception as e:
        print(e)
    return True
	
def getColumnName(csvPath):
    print('Getting Column Names from csv......')
    cmd = 'hdfs dfs -cat '+csvPath+' | head -n 1'  
    print(cmd)
    result = countValidation.runCommand(cmd)  
    a=result.stdout
    b=a.replace('\n','')
    colNames=b.split(',')    
    return colNames

def getHiveCount():
    #cmd2 = 'hdfs dfs -cat /user/hive/warehouse/'+dbName+'.db/'+tname+'/part-m-* | wc -l'  
    cmd2 = 'hdfs dfs -cat /user/hive/warehouse/'+dbName+'.db/'+tname.lower()+'/'+csvName+' | wc -l' 
    result2 = countValidation.runCommand(cmd2)
    outString2=result2.stdout
    hiveCount= countValidation.getCount(outString2)
    print('Hive table count is:',hiveCount)    
    return hiveCount

def runsqlfile():
    print('SQL file is running.....')
    cmd = 'hive -f hive_Tbl_create.sql'
    res = countValidation.runCommand(cmd)
    return res