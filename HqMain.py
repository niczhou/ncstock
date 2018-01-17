#coding=utf-8
import pymysql
import time
import datetime
import json
from HqAnalyst import HqAnalyst
from HqUtil import HqUtil
from HsUpdater import HsUpdater
from HqUpdater import HqUpdater
import threadpool

conn = pymysql.connect(host="localhost",user="root",passwd="",db="nxstock",charset="utf8")
cursor=conn.cursor()

def initDB():
    mHsUpdater=HsUpdater(conn)
    mHsUpdater.createTableHs("tablesh")
    mHsUpdater.createTableHs("tablesz")   
    mHsUpdater.updateTableHs("tablesh","res/20180107listsh.xlsx")
    mHsUpdater.updateTableHs("tablesz","res/20180107listsz.xlsx")
    mHsUpdater.updateTableCodesByHs("tablesz")
    mHsUpdater.updateTableCodesByHs("tablesz")
    mHsUpdater.createTableDate("tabledate")
    
def updaterHq():
    mHsUpdater=HsUpdater(conn)
    mHqUpdater=HqUpdater(conn)
    dt=time.strftime("%Y%d%m",time.localtime())
    startDate=mUtil.getEndDate(dt,conn)
    mHqUpdater.updateHqByHs("tablesz",startDate,dt)
    mHqUpdater.updateHqByHs("tablesh",startDate,dt)
    mHsUpdater.updateDateTable()
    
def ifBuyToday(HqStrategy=0):
    mAnalyst=HqAnalyst(conn)    
    dt=int(time.strftime("%Y%m%d",time.localtime()))
    mAnalyst.ifBuyTodayByHs("tablesz",dt,HqStrategy)
    mAnalyst.ifBuyTodayByHs("tablesh",dt,HqStrategy)
    
def ifBuyAlltime(HqStrategy=0): 
    mAnalyst=HqAnalyst(conn)    
    mAnalyst.ifBuyAlltimeByHs("tablesz",HqStrategy)
#     mAnalyst.ifBuyAlltimeByHs("tablesh",HqStrategy)   
    
# ifBuyToday(1)
ifBuyAlltime(0)

conn.close()
