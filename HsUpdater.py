#coding=utf-8

import xlrd
import pymysql
from HqUtil import HqUtil


class HsUpdater:
    __conn=None
    __cursor=None
    
    def __init__(self,connection):
        self.__conn=connection
        self.__cursor=connection.cursor()
        
    def updateListsz(self,xlPath):       
        mBook=xlrd.open_workbook(xlPath)    
        mSheet=mBook.sheets()[0]
        
        for cel in mSheet.col_values(5):
            if cel!="A股代码":        
                sql="SELECT stock_code FROM listsz WHERE stock_code ='"+str(cel)+"'"
                self.__cursor.execute(sql)
                result=self.__cursor.fetchone()            
                if result==None:
                    sql="INSERT INTO listsz (stock_code) VALUES ('" +str(cel)+"')"
                    print(cel+":"+sql)
                    self.__cursor.execute(sql)
    
    def updateListsh(self,xlpath):
        mBook=xlrd.open_workbook(xlpath)
        mSheet=mBook.sheets()[0]
        
        for cel in mSheet.col_values(2):
            if cel!="A股代码":
                sql="SELECT stock_code FROM listsh WHERE stock_code ='"+str(cel)+"'"
                self.__cursor.execute(sql)
                result=self.__cursor.fetchone()
                if result==None:
                    sql="INSERT INTO listsz(stock_code) VALUES('"+str(cel)+"')"
                    print(cel+":"+sql)
                    self.__cursor.execute(sql)
                
    def createList(self,shsz):
        try:
            sql="CREATE TABBLE list"+shsz+"(ID INT NOT NULL AUTO_INCREMENT, \
            stock_code VARCHAR(255),stock_name VARCHAR(255),stock_ipo VARCHAR(255), \
            stock_total VARCHAR(255),stock_circulation VARCHAR(255),PRIMARY KEY(ID))" 
            self.__cursor.execute(sql)            
        except:
            print("can not CREATE table list"+shsz)
            
        
        
        
        
        