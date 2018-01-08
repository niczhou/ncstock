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
                sq="SELECT stock_code FROM listsz WHERE stock_code ='"+str(cel)+"'"
                self.__cursor.execute(sq)
                result=self.__cursor.fetchone()            
                if result==None:
                    sq="INSERT INTO listsz (stock_code) VALUES ('" +str(cel)+"')"
#                     print(cel+":"+sql)
                    self.__cursor.execute(sq)
    
    def updateListsh(self,xlpath):
        print("updatesh")
        mBook=xlrd.open_workbook(xlpath)
        mSheet=mBook.sheets()[0]
        
        for cel in mSheet.col_values(2):
            if cel!="A股代码":
                sql="SELECT stock_code FROM listsh WHERE stock_code ='"+str(int(cel))+"'"
                self.__cursor.execute(sql)
                result=self.__cursor.fetchone()
                if not result:
                    sql="INSERT INTO listsh(stock_code) VALUES('"+str(int(cel))+"')"
#                     print(cel+":"+sql)
                    self.__cursor.execute(sql)
                
    def createList(self,shsz):
        try:
            sq="CREATE TABLE IF NOT EXISTS list"+shsz+"(ID INT NOT NULL AUTO_INCREMENT,"\
            +"stock_code VARCHAR(32),stock_name VARCHAR(255),stock_ipo VARCHAR(64),"\
            +"stock_total VARCHAR(255),stock_circulation VARCHAR(255),PRIMARY KEY(ID))" 
            print(sq)
            self.__cursor.execute(sq)            
        except:
            print("can not CREATE table list"+shsz)
            
    def updateCodeTables(self):
        sq="SELECT stock_code FROM listsh"
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listSh=[result[i][0] for i in range(len(result))]
        for j in range(len(listSh)):
            self.createCodeTable(listSh[j])
        
        sq="SELECT stock_code FROM listsz"
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listSz=[result[i][0] for i in range(len(result))]
        for j in range(len(listSz)):
            self.createCodeTable(listSz[j])
        
    def createCodeTable(self,stockCode):
        try:
            sq="CREATE TABLE IF NOT EXISTS `"+stockCode+"`(ID INT NOT NULL AUTO_INCREMENT,"\
                +"trade_date VARCHAR(128),`open` VARCHAR(128),`close` VARCHAR(128),`change`"\
                +" VARCHAR(128),`percent` VARCHAR(128),`low` VARCHAR(128),`high` VARCHAR(128),"\
                +"volume VARCHAR(255),amount VARCHAR(255),turnover VARCHAR(128),PRIMARY KEY(ID));"
            print(sq)
            self.__cursor.execute(sq)    
        except:
            print("can not CREATE table "+stockCode)
        
        