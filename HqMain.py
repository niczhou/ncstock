#coding=utf-8
import pymysql
import time
import datetime
import json
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil
from HsUpdater import HsUpdater
# from HsWorm import HsWorm
from HqUpdater import HqUpdater
import threadpool
from test._test_multiprocessing import exception_throwing_generator

conn = pymysql.connect(host="localhost",user="root",passwd="",db="stock",charset="utf8")
cursor=conn.cursor()

mAnalyst=HqAnalyst(conn)
mUtil = HqUtil()
  
sq="SELECT stock_code FROM listsz"
cursor.execute(sq)
tupleSh=cursor.fetchall()
listSh=[tupleSh[i][0] for i in range(len(tupleSh))]
 
# print(mAnalyst.getAvgByIndex(listSh[0],'amount',"20171111","20180103"))
# # mAnalyst.getIsBuyByClose(listSh[3],"20171111","20180103")
# mAnalyst.getIsBuyByAmount(listSh[0],"20171111","20180103")
#  
for i in range(len(listSh)):
    mAnalyst.getIsBuyByClose(listSh[i],"20171111","20180103")
#     mAnalyst.getIsBuyByAmount(listSh[i],"20171111","20180104")


# def analSh(stockCode):
# #     print(str(stockCode))
#     mAnalyst = HqAnalyst(conn)
#     mAnalyst.getIsBuyByClose(stockCode,"20171026","20180103")
#      
# pool=threadpool.ThreadPool(8)
# requests=threadpool.makeRequests(analSh, listSh)
# [pool.putRequest(req) for req in requests]
# pool.wait()


# stockIndex='amount'
# stockCode='600000'
# startDate='20171111'
# endDate='20180103'
# __sq="SELECT MIN(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"+str(startDate)+" AND trade_date<"+str(endDate)
# cursor.execute(__sq)
# res=cursor.fetchone()
# print(str(type(res[0])))
      
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


conn.close()
