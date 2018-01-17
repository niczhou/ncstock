#coding=utf-8
import pymysql
import time
import datetime
import json
from HqShortAnalyst import HqShortAnalyst
from HqUtil import HqUtil
from HsUpdater import HsUpdater
from HqUpdater import HqUpdater
import threadpool

conn = pymysql.connect(host="localhost",user="root",passwd="",db="nxstock",charset="utf8")
cursor=conn.cursor()

def ifBuyForShort():
    mUtil=HqUtil()
    sAnalyst=HqShortAnalyst(conn)    
    dt=int(time.strftime("%Y%d%m",time.localtime()))
    sAnalyst.ifBuyByHs("tablesz",dt)
    sAnalyst.ifBuyByHs("tablesh",dt)
    
def updaterHq():
    mHsUpdater=HsUpdater(conn)
    mHqUpdater=HqUpdater(conn)
    dt=time.strftime("%Y%d%m",time.localtime())
    startDate=mUtil.getEndDate(dt,conn)
    mHqUpdater.updateHqByHs("tablesz",startDate,dt)
    mHqUpdater.updateHqByHs("tablesh",startDate,dt)
    mHsUpdater.updateDateTable()
    
def initDB():
    mHsUpdater=HsUpdater(conn)
    mHsUpdater.createTableHs("tablesh")
    mHsUpdater.createTableHs("tablesz")   
    mHsUpdater.updateTableHs("tablesh","res/20180107listsh.xlsx")
    mHsUpdater.updateTableHs("tablesz","res/20180107listsz.xlsx")
    mHsUpdater.updateTableCodesByHs("tablesz")
    mHsUpdater.updateTableCodesByHs("tablesz")
    mHsUpdater.createTableDate("tabledate")
    
ifBuyForShort()

conn.close()
