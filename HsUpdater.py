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

    def createTableHs(self,hsTableName):
        sq="CREATE TABLE IF NOT EXISTS "+hsTableName+"(id INT NOT NULL AUTO_INCREMENT,"\
        +"stock_code VARCHAR(8),stock_name VARCHAR(16),stock_ipo INT,"\
        +"stock_total BIGINT,stock_circulation BIGINT,stock_url VARCHAR(255),PRIMARY KEY(id), UNIQUE(stock_code,stock_name))" 
#             print(sq)
        self.__cursor.execute(sq)            
        
    def updateTableHs(self,tableHs,xlPath):       
        mBook=xlrd.open_workbook(xlPath)    
        mSheet=mBook.sheets()[0]
        
        if tableHs=="tablesh":
            listCode=[str(int(mSheet.col_values(2)[i])) for i in range(1,len(mSheet.col_values(2)))]
        else:
            listCode=[mSheet.col_values(5)[i] for i in range(1,len(mSheet.col_values(5)))]
            
        for cel in listCode:
            if cel and cel!="A股代码":        
                sq="INSERT INTO "+tableHs+"(stock_code) SELECT '"+str(cel)+"' FROM dual "\
                    +"WHERE NOT EXISTS(SELECT stock_code FROM "+tableHs+" WHERE stock_code="+str(cel)+")"                
#                 print(str(cel)+":"+sq)
                self.__cursor.execute(sq)
######################################################################################
    def createTableCode(self,stockCode):
        sq="CREATE TABLE IF NOT EXISTS `"+stockCode+"`(id INT NOT NULL AUTO_INCREMENT,"\
            +"trade_date INT,`open` FLOAT,`close` FLOAT,`change`"\
            +" FLOAT,`percent` FLOAT,`low` FLOAT,`high` FLOAT,"\
            +"volume DOUBLE,amount DOUBLE,turnover FLOAT,PRIMARY KEY(id),UNIQUE(trade_date))"
        print(sq)
        self.__cursor.execute(sq)    
             
    def updateTablesHs(self,tableHs):
        if tableHs=="tablesh" or tableHs=="tablesz":
            sq="SELECT stock_code FROM " +tableHs     
            self.__cursor.execute(sq)
            result=self.__cursor.fetchall()
            listHs=[result[i][0] for i in range(len(result))]
            for j in range(len(listHs)):
                self.createTableCode(listHs[j])
        else:
            print("can not get codes hs")
##############################################################################
    def createTableDate(self,tableDate):
        sq="CREATE TABLE IF NOT EXISTS "+tableDate+"(id INT NOT NULL AUTO_INCREMENT,"\
            +"trade_date INT,PRIMARY KEY(id),UNIQUE(trade_date))"
#         print(sq)
        self.__cursor.execute(sq)
    
        
        