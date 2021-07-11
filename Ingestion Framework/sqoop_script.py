
#Importing All Packages
#----------------------------

import json
import subprocess

#Connecting with Config File
#----------------------------

with open('config.json') as cj:
    val = json.load(cj)


#Exception Handeling  
#----------------------------

try:
    query='''sqoop import  --connect {c} --username {un} --password {pd} --table {tb} --m 1 --target-dir {td} --hive-import --create-hive-table'''.format(c=val['conn'], 
                                                                                                                                                        un=val['username'],                                                                                                                                                            pd=val['password'], 
                                                                                                                                                        tb=val['tabName'], 
                                                                                                                                                        td=val['tarDir'],)

    subprocess.run(query, shell=True)

except Exception as ex:
    print(ex)
