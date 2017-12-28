#coding=utf-8

import pymysql
import time
import datetime
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil
from HsUpdater import HsUpdater
from HsWorm import HsWorm

conn = pymysql.connect(host="localhost",user="root",passwd="",db="stock",charset="utf8")
cursor=conn.cursor()

mAnalyst = HqAnalyst(conn)
mUtil = HqUtil()

# sql="SELECT stock_code FROM listsh"
# cursor.execute(sql)
# shArr=cursor.fetchall()
# print(str(len(shArr)))
# for i in range(0,len(shArr)):
#   try:
#     isBuy=mAnalyst.getIsBuyByCode(str(shArr[i][0]),"20170922",11)
#     print(str(shArr[i][0])+":"+isBuy)
#   except:
#     continue

sql="SELECT stock_code FROM listsz"
cursor.execute(sql)
szArr=cursor.fetchall()
print(str(len(szArr)))
for i in range(0,len(szArr)):
  try:
    isBuy=mAnalyst.getIsBuyByCode(str(szArr[i][0]),"20170918",7)
#     print(str(szArr[i][0])+":"+isBuy)
  except:
    continue

# startDate=mUtil.getStartDate("20170922",9,conn)
# print(startDate)
# bUrl="http://www.fjbs.gov.cn/eWebEditor/uploadfile/20171103100531556.xlsx"
# mWorm=HsWorm()
# 
# mWorm.downloadf(bUrl)
# mWorm.readUrl('http://www.fjbs.gov.cn/eWebEditor/uploadfile/20171103100531556.xlsx')

# sql="SELECT stock_code FROM listsh"
# cursor.execute(sql)
# stockArr=cursor.fetchall()
# print(len(stockArr))

# for i in range(0,len(stockArr)):
#   try:
#     isBuy=mAnalyst.getIsBuyByCode(str(stockArr[i][0]),"20170922",17)
#     print(str(stockArr[i][0])+":"+isBuy)
#   except:
#     continue
# 
# mUtil=HqUtil()
# print(mUtil.getStartDate("20171127",7,conn))
# 
# mHs=HsUpdater(conn)
# mHs.createList("sh")
# mHs.updateListsz('C:/demo/nzstock/input/20171203深圳A股列表.xlsx')
# mHs.updateListsh('C:/demo/nzstock/input/20171203上海A股列表.xlsx')

# a=time.time()
# print(a)
# b=time.strptime(a,"%Y%m%d")
# print(a+"=>"+b)


conn.close()
