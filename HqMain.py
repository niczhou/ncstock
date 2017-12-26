#coding=utf-8

import pymysql
import time
import datetime
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil
from HsUpdater import HsUpdater
from HsWorm import HsWorm

conn = pymysql.connect(host="localhost",user="root",passwd="",db="stock",charset="utf8")

mAnalyst = HqAnalyst(conn)

# bUrl="http://www.fjbs.gov.cn/eWebEditor/uploadfile/20171103100531556.xlsx"
# mWorm=HsWorm()
# 
# mWorm.downloadf(bUrl)
# mWorm.readUrl('http://www.fjbs.gov.cn/eWebEditor/uploadfile/20171103100531556.xlsx')

sql="SELECT stock_code FROM listsz"
cursor=conn.cursor()
cursor.execute(sql)
stockArr=cursor.fetchall()
print(len(stockArr))

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
mHs=HsUpdater(conn)
mHs.updateListsz('C:/demo/nzstock/input/20171203深圳A股列表.xlsx')
mHs.updateListsh('C:/demo/nzstock/input/20171203上海A股列表.xlsx')

# a=time.time()
# print(a)
# b=time.strptime(a,"%Y%m%d")
# print(a+"=>"+b)


conn.close()
