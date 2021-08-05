import re
from subprocess import run,PIPE


def getCount(std):
    count = re.findall(r'\d+', std)[0]
    count=int(count)
    return count


def runSequence(cmd):
    output = run(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return output



def countCheck(val):

    #SPLITTING HIVE DATABASE and TABLE NAME
    #---------------------------------------
    dbLoc = val['hiveName'].split(".")
    db = dbLoc[0]
    tname = dbLoc[1].lower()

    try:

        print("CHECKING FOR SUCCESSFUL IMPORT OF ALL RECORDS.....")
        query='''sqoop eval \
            --connect {conn} \
            --username {usr} \
            --password {pwd}
            --query "select count(*) from {tnm}"'''.format (conn=val['connectionURL'],
                                                                usr=val['usrName'],
                                                                pwd=val['pWord'],
                                                                tnm=val['mysqlName'] )


        output = runSequence(query)
        std = output.stdout
        print(output.returncode)
        if output.returncode==0:
        
            #ROW COUNT FOR MySQL TABLE
            mysqlCount = getCount(std)
            print('MySql table count is:', mysqlCount)

            #ROW COUNT FOR HIVE TABLE
            cmd2 = 'hdfs dfs -cat /user/hive/warehouse/'+db+'.db/'+tname+'/part-m-* | wc -l'
            result2=runSequence(cmd2)
            outString2 = result2.stdout
            hiveCount = getCount(outString2)
            print('Hive table count is:', hiveCount)

            if mysqlCount == hiveCount :
                print(".......... ROW COUNT OF MySQL & HIVE TABLE, MATCHES ............")
            elif mysqlCount > hiveCount :
                print('WHILE IMPORTING ROW COUNT IS DROPPED BY ', (mysqlCount-hiveCount),' RECORDS')
            else:
                print('WHILE IMPORTING ROW COUNT IS INCREASED BY ', (hiveCount-mysqlCount),'RECORDS')

    except Exception as ex:
        print(ex)
