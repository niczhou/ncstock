import pymysql
import time
from HqAnalyst import HqAnalyst

conn = pymysql.connect("localhost","root","","stock")

mAnalyst = HqAnalyst(conn)

isBuy=mAnalyst.getIsBuyByAmount("000011","20170801","20170922")
print(isBuy)


conn.close()
