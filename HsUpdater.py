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
        try:
            sq="CREATE TABLE IF NOT EXISTS "+hsTableName+"(id INT NOT NULL AUTO_INCREMENT,"\
            +"stock_code VARCHAR(8),stock_name VARCHAR(16),stock_ipo INT,"\
            +"stock_total BIGINT,stock_circulation BIGINT,PRIMARY KEY(id))" 
            print(sq)
            self.__cursor.execute(sq)            
        except:
            print("can not CREATE table list"+shsz)
        
    def updateTableHs(self,shsz,xlPath):       
        mBook=xlrd.open_workbook(xlPath)    
        mSheet=mBook.sheets()[0]
        
        if shsz=="tablesz":
            listCode=mSheet.col_values(5)
        else:
            listCode=mSheet.col_values(2)
            
        for cel in listCode:
            if cel!="A股代码":        
                sq="INSERT INTO "+shsz+"(stock_code) VALUES ('" +str(cel)+"')"
#                     +"WHERE NOT EXISTS(SELECT stock_code FROM "+shsz+" WHERE stock_code="+cel+")"
#                 sq="INSERT INTO "+shsz+"(stock_code) VALUES ('" +str(cel)+"')"\
#                     +"WHERE NOT EXISTS(SELECT stock_code FROM "+shsz+" WHERE stock_code="+cel+")"                
#                     print(cel+":"+sql)
                self.__cursor.execute(sq)
    
#     def updateTableSh(self,xlpath):
#         print("updatesh")
#         mBook=xlrd.open_workbook(xlpath)
#         mSheet=mBook.sheets()[0]
#         
#         for cel in mSheet.col_values(2):
#             if cel!="A股代码":
#                 sql="SELECT stock_code FROM tablesh WHERE stock_code ='"+str(int(cel))+"'"
#                 self.__cursor.execute(sql)
#                 result=self.__cursor.fetchone()
#                 if not result:
#                     sql="INSERT INTO tablesh(stock_code) VALUES('"+str(int(cel))+"')"
# #                     print(cel+":"+sql)
#                     self.__cursor.execute(sql)
######################################################################################
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
        

        
        