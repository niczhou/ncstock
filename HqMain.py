#coding=utf-8
import pymysql
import time
import datetime
import json
import xlrd
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil
from DBUpdater import DBUpdater
from HqUpdater import HqUpdater
import threadpool

conn = pymysql.connect(host="localhost",user="root",passwd="",db="nxstock",charset="utf8")
cursor=conn.cursor()

def initDB():
    dUpdater=DBUpdater(conn)
    dUpdater.createTableHs("tablesh")
    dUpdater.createTableHs("tablesz")   
    dUpdater.updateTableHs("tablesh","res/20180107listsh.xlsx")
    dUpdater.updateTableHs("tablesz","res/20180107listsz.xlsx")
    dUpdater.updateTableCodesByHs("tablesz")
    dUpdater.updateTableCodesByHs("tablesz")
    dUpdater.createTableDate("tabledate")
    
def updaterHq():
    dUpdater=DBUpdater(conn)
    mHqUpdater=HqUpdater(conn)
    mUtil=HqUtil()
    dt=time.strftime("%Y%d%m",time.localtime())
    if mHqUpdater.ifUpdated(dt):        
        print("DB already updated!")
    else:
        print("start updating")
        startDate=mUtil.getEndDate(dt,conn)
        mHqUpdater.updateHqByHs("tablesz",startDate,dt)
        mHqUpdater.updateHqByHs("tablesh",startDate,dt)
        dUpdater.updateTableDate()
    
def ifBuyToday(HqStrategy=0):
    mAnalyst=HqAnalyst(conn)    
    dt=int(time.strftime("%Y%m%d",time.localtime()))
    mAnalyst.ifBuyTodayByHs("tablesz",dt,HqStrategy)
    mAnalyst.ifBuyTodayByHs("tablesh",dt,HqStrategy)
    
def ifBuyAlltime(HqStrategy=0): 
    mAnalyst=HqAnalyst(conn)    
    mAnalyst.ifBuyAlltimeByHs("tablesz",HqStrategy)
    mAnalyst.ifBuyAlltimeByHs("tablesh",HqStrategy)
   
updaterHq()    
# ifBuyToday(0)
# ifBuyAlltime(1)

conn.close()
