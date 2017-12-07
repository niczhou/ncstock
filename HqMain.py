import pymysql
import time
from HqAnalyst import HqAnalyst

conn = pymysql.connect("localhost","root","","stock")
##cursor=conn.cursor()
##cursor.execute("SELECT trade_date,`close` FROM `000001` WHERE trade_date<20170616")
##var1=cursor.fetchall()
##print(var1[1][1])

mAnalyst = HqAnalyst(conn)
result=mAnalyst.getMinByIndex('000001','close','20170901','20170922')
mAnalyst.getDateByIndexValue('000001','close',str(result))
result=mAnalyst.getMaxByIndex('000001','close','20170901','20170922')
mAnalyst.getDateByIndexValue('000001','close',str(result))
result=mAnalyst.getMinMaxByIndex('000001','close','20170901','20170922')
date=time.strftime("%Y%m%d")
print(date)

dateDiff=mAnalyst.getDateDiff('20171201','20171211')
print(dateDiff)


conn.close()
