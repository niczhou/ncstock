import pymysql
import time
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil

conn = pymysql.connect("localhost","root","","stock")

mAnalyst = HqAnalyst(conn)

for i in range(10,100):
  try:
    isBuy=mAnalyst.getIsBuyByCode("6000"+str(i),"20170801","20170922")
    print("6000"+str(i)+":"+isBuy)
  except:
    continue

mUtil=HqUtil()
m=mUtil.getStartDate("20171127",7,conn)

conn.close()
