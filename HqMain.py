#coding=utf-8

import pymysql
import time
import datetime
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil
from HsUpdater import HsUpdater

conn = pymysql.connect(host="localhost",user="root",passwd="",db="stock",charset="utf8")

mAnalyst = HqAnalyst(conn)

# for i in range(10,100):
#   try:
#     isBuy=mAnalyst.getIsBuyByCode("6000"+str(i),"20170922",17)
#     print("6000"+str(i)+":"+isBuy)
#   except:
#     continue
# 
mUtil=HqUtil()
print(mUtil.getStartDate("20171127",7,conn))

mHs=HsUpdater(conn)
# mHs.updateListsz('C:/demo/nzstock/input/20171203深圳A股列表.xlsx')
a=time.time()
print(a)
b=time.strptime(a,"%Y%m%d")
print(a+"=>"+b)


conn.close()
