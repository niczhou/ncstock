#coding=utf-8
import pymysql
import time
import datetime
import json
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil
from HsUpdater import HsUpdater
from HsWorm import HsWorm
from HqUpdater import HqUpdater

with open('return.txt', 'r') as f:
  data = json.load(f)
  print(isinstance(data,list))
  hq=data[0]['hq']
  print(isinstance(hq,list))
mUpdater=HqUpdater()
mUpdater.getHqlist(hq)



# conn = pymysql.connect(host="localhost",user="root",passwd="",db="stock",charset="utf8")
# cursor=conn.cursor()
# 
# mAnalyst = HqAnalyst(conn)
# mUtil = HqUtil()
# 
# sql="SELECT stock_code FROM listsz"
# cursor.execute(sql)
# shArr=cursor.fetchall()
# print(str(len(shArr)))
# for i in range(0,len(shArr)):
#   try:
#     isBuy=mAnalyst.getIsBuy(shArr[i][0],"20170922",11)
#     print(shArr[i][0]+":"+isBuy)
#   except:
#     continue

# print(str(mAnalyst.getAdjustedRatioByClose("000002","20170720","20170818")))
# print(mAnalyst.getMinByIndex("000001","close","20170808","20170922"))
# print(mAnalyst.getMinByIndex("000001","amount","20170808","20170922"))
#  
# print(mAnalyst.getDateByIndexValue("000001","close","10.02"))
# print(mAnalyst.getDateByIndexValue("000001","amount","41765.31"))
#  
# print(mAnalyst.getMaxByIndex("000001","close","20170808","20170922"))
# print(mAnalyst.getMaxByIndex("000001","amount","20170808","20170922"))
# print(mAnalyst.getAvgByIndex("000001","close","20170808","20170922"))
# print(mAnalyst.getAvgByIndex("000001","amount","20170808","20170922"))
#  
# print(mAnalyst.getDateDiff("20170922","20170808"))
#  
# print(str(mAnalyst.getIsBuyByClose("000001","20170808","20170922")))
# print(str(mAnalyst.getIsBuyByAmount("000001","20170808","20170922")))
# print(str(mAnalyst.getIsBuy("000016","20170922",33)))


# sq="SELECT stock_code FROM listsz"
# cursor.execute(sq)
# szArr=cursor.fetchall()
# print(str(len(szArr)))
# for i in range(0,len(szArr)):
#   try:
# #     print(str(szArr[i][0]))
#     isBuy=mAnalyst.getIsBuyByCode(str(szArr[i][0]),"20170922",7)
#     print(str(szArr[i][0])+":"+str(isBuy))
#   except:
#     continue

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


# conn.close()
