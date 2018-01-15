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

conn = pymysql.connect(host="localhost",user="root",passwd="",db="nxstock",charset="utf8")
cursor=conn.cursor()
 
mHsUpdater=HsUpdater(conn)
mHqUpdater=HqUpdater(conn)
mAnalyst=HqAnalyst(conn)
mUtil = HqUtil()

mAnalyst.getIsBuyByCode("000010")
# mHsUpdater.createTableHs("tablesh")
# mHsUpdater.createTableHs("tablesz")   
# mHsUpdater.updateTableHs("tablesh","res/20180107listsh.xlsx")
# mHsUpdater.updateTableHs("tablesz","res/20180107listsz.xlsx")
# mHsUpdater.createTableDate("tableDate") 
# mHsUpdater.updateTablesHs("tablesh")
# mHsUpdater.updateTablesHs("tablesz")
# mHqUpdater.updateHqByHs("tablesz")
# mHqUpdater.updateHqByHs("tablesh")
# mHsUpdater.updateTableDate()

#  mAnalyst.getIsBuyByHs("tablesz",20180110,17)
# sq="SELECT stock_code FROM tablesz"
# #     try:
# cursor.execute(sq)
# result=cursor.fetchall()  
# listSz=[result[i][0] for i in range(len(result))]
# # 
# for codeSz in listSz:
# #     pass
#     mAnalyst.getIsBuyByClose(codeSz,20171208,20180110)
#     mAnalyst.getIsBuyByAmount(codeSz,20171208,20180110)
#     mAnalyst.getIsBuyByCode(codeSz,20171208,20180110)

# def analSh(stockCode):
# #     print(str(stockCode))
#     mAnalyst = HqAnalyst(conn)
#     mAnalyst.getIsBuyByClose(stockCode,"20171026","20180103")
#      
# pool=threadpool.ThreadPool(8)
# requests=threadpool.makeRequests(analSh, listSh)
# [pool.putRequest(req) for req in requests]
# pool.wait()

# __sq="SELECT MIN(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"+str(startDate)+" AND trade_date<"+str(endDate)
# cursor.execute(__sq)
# res=cursor.fetchone()
# print(str(type(res[0])))  
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

# a=time.time()
# print(a)
# b=time.strptime(a,"%Y%m%d")
# print(a+"=>"+b)


conn.close()
