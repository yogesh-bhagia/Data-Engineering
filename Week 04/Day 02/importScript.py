import sys
import json
import subprocess
from subprocess import run,PIPE
import countValidation
 
with open("config.json") as f:
    jsonData = json.load(f)


def tableImport():
    try:
        cmd="""sqoop import \
    --connect {connectionstr} \
    --username {id} \
    --password {passwrd} \
    --table {tname} \
    --target-dir {trgtDir} \
    --hive-import \
    --create-hive-table \
    --hive-table {dbname}""".format(
            connectionstr=jsonData["connURL"],
            id=jsonData["userName"],
            passwrd=jsonData["passWord"],
            tname=jsonData["mysqlTblName"],
            trgtDir=jsonData["targetDir"],
            dbname=jsonData["hiveTblName"],
        )

        # subprocess.run(cmd, shell=True)
        result=countValidation.runCommand(cmd)
        if result.returncode == 0:
            countValidation.countCheck(jsonData)
        else:
            print("Sqoop command executed IMPROPERLY")
            sys.exit(1)

    except Exception as e:
        print(e)