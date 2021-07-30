import sys
import json
import subprocess
from subprocess import run, PIPE
import checkPerform

with open("config.json") as cd:
        val = json.load(cd)
        
def sqoopImport():
    try:
        cmd='''sqoop import \
        --connect {conn} \
        --username {usr} \
        --password {pwd} \
        --table {tnm} \
        --target-dir {tdr} \
        --create-hive-table \
        --hive-import \
        --hive-table {htn}'''.format(conn=val['connectionURL'], 
                                      usr=val['usrName'],
                                      pwd=val['pWord'], 
                                      tnm=val['mysqlName'], 
                                      tdr=val['targetDir'],
                                      htn=val['hiveName'])
        subprocess.run(cmd, shell=True)
    
        checkPerform.countCheck(val)



    except Exception as ex:
        print(ex)    
        
sqoopImport()