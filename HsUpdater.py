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
                sq="SELECT stock_code FROM tablesz WHERE stock_code ='"+str(cel)+"'"
                self.__cursor.execute(sq)
                result=self.__cursor.fetchone()            
                if result==None:
                    sq="INSERT INTO tablesz (stock_code) VALUES ('" +str(cel)+"')"
#                     print(cel+":"+sql)
                    self.__cursor.execute(sq)
    
    def updateListsh(self,xlpath):
        print("updatesh")
        mBook=xlrd.open_workbook(xlpath)
        mSheet=mBook.sheets()[0]
        
        for cel in mSheet.col_values(2):
            if cel!="A股代码":
                sql="SELECT stock_code FROM tablesh WHERE stock_code ='"+str(int(cel))+"'"
                self.__cursor.execute(sql)
                result=self.__cursor.fetchone()
                if not result:
                    sql="INSERT INTO tablesh(stock_code) VALUES('"+str(int(cel))+"')"
#                     print(cel+":"+sql)
                    self.__cursor.execute(sql)
                
    def createTableHs(self,shsz):
        try:
            sq="CREATE TABLE IF NOT EXISTS table"+shsz+"(id INT NOT NULL AUTO_INCREMENT,"\
            +"stock_code VARCHAR(4),stock_name VARCHAR(16),stock_ipo INT,"\
            +"stock_total BIGINT,stock_circulation BIGINT,PRIMARY KEY(id))" 
            print(sq)
            self.__cursor.execute(sq)            
        except:
            print("can not CREATE table list"+shsz)
            
    def updateCodeTables(self):
        sq="SELECT stock_code FROM tablesh"
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listSh=[result[i][0] for i in range(len(result))]
        for j in range(len(listSh)):
            self.createCodeTable(listSh[j])
        
        sq="SELECT stock_code FROM tablesz"
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listSz=[result[i][0] for i in range(len(result))]
        for j in range(len(listSz)):
            self.createCodeTable(listSz[j])
        
    def createCodeTable(self,stockCode):
        try:
            sq="CREATE TABLE IF NOT EXISTS `"+stockCode+"`(id INT NOT NULL AUTO_INCREMENT,"\
                +"trade_date int,`open` FLOAT,`close` FLOAT,`change`"\
                +" FLOAT,`percent` FLOAT,`low` FLOAT,`high` FLOAT,"\
                +"volume DOUBLE,amount DOUBLE,turnover FLOAT,PRIMARY KEY(ID));"
            print(sq)
            self.__cursor.execute(sq)    
        except:
            print("can not CREATE table "+stockCode)
        
        