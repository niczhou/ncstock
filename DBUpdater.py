#coding=utf-8

import xlrd
import pymysql
from HqUtil import HqUtil


class DBUpdater:
    __conn=None
    __cursor=None
    
    def __init__(self,connection):
        self.__conn=connection
        self.__cursor=connection.cursor()

    def createTableHs(self,hsTableName):
        sq="CREATE TABLE IF NOT EXISTS "+hsTableName+"(id INT NOT NULL AUTO_INCREMENT,"\
        +"stock_code VARCHAR(10),stock_name VARCHAR(12),stock_ipo INT,"\
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
            +"trade_date INT,`open` DECIMAL(8,2),`close` DECIMAL(8,2),`change` DECIMAL(8,2),"\
            +"`percent` DECIMAL(6,2),`low` DECIMAL(8,2),`high` DECIMAL(8,2),"\
            +"volume BIGINT,amount DECIMAL(12,2),turnover DECIMAL(6,2),PRIMARY KEY(id),UNIQUE(trade_date))"
        print(sq)
        self.__cursor.execute(sq)    
             
    def updateTableCodesByHs(self,tableHs):
        if tableHs=="tablesh" or tableHs=="tablesz":
            sq="SELECT stock_code FROM " +tableHs     
            self.__cursor.execute(sq)
            result=self.__cursor.fetchall()
            listHs=[result[i][0] for i in range(len(result))]
            for j in range(len(listHs)):
                self.createTableCode(listHs[j])
        else:
            print("can not get codes hs")
#####################################################################################
    def createTableDate(self,tableDate):
        sq="CREATE TABLE IF NOT EXISTS "+tableDate+"(id INT NOT NULL AUTO_INCREMENT,"\
            +"trade_date INT,PRIMARY KEY(id),UNIQUE(trade_date))"
#         print(sq)
        self.__cursor.execute(sq)
    
    def updateTableDate(self):
        sq="SELECT stock_code FROM tablesz LIMIT 12"
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listTest=[result[i][0] for i in range(len(result))]
        
        lenDate=0
        listDate=[]
        for testCode in listTest:
            sq="SELECT trade_date FROM `"+testCode+"`"
            self.__cursor.execute(sq)
            result=self.__cursor.fetchall()
            if lenDate<len(result):
                lenDate=len(result)
                listDate=[result[i][0] for i in range(len(result))]
        
#         print("%d"%lenDate+str(listDate))
        for uDate in listDate:
            sq="INSERT INTO tableDate(trade_date) SELECT '"+str(uDate) \
                +"' FROM dual WHERE NOT EXISTS(SELECT trade_date FROM tabledate WHERE trade_date="\
                +str(uDate)+")"
            try:
                self.__cursor.execute(sq)
            except:
                pass
#                 print("update fail %d"+uDate)          
        