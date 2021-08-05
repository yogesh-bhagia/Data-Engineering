

import json
import countValidation

with open("config.json") as f:
    jsonData = json.load(f)
    csvName = jsonData["csvName"]
    tname = csvName.split(".")[0]
    dbName = jsonData["hiveDBname"]
    csvPath = jsonData['hdfs_csv_location']+jsonData['csvName']

def upload_File_To_Haddop():
    print('Uploading to hdfs.........')
    cmd='hdfs dfs -put /home/ybhagia2005/csvFiles/'+csvName+' '+csvPath
    print(cmd)
    result = countValidation.runCommand(cmd)    
    return result

def checkFileCount(csvPath):
    print('Checking count of uploaded file......')
    cmd = 'hdfs dfs -cat '+csvPath+' | wc -l'
    result = countValidation.runCommand(cmd)
    count = countValidation.getCount(result.stdout)
    print('Uploaded files count is: ',count)
    return count