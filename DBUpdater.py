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
############################################################################################
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
##########################################################################################
    def createTableOut(self,tableName): 
        sq="CREATE TABLE IF NOT EXISTS "+str(tableName)+"(id INT NOT NULL AUTO_INCREMENT,stock_code VARCHAR(10),"\
            +"stock_name VARCHAR(12),PRIMARY KEY(id),UNIQUE(stock_code,stock_name))"
        print(sq)
        self.__cursor.execute(sq)

    def updateTableOut(self,shsz):
        if shsz=='sh':
            tableOut='tableoutsh'
            tableHs='tablesh'
        elif shsz=='sz':
            tableOut='tableoutsz'
            tableHs='tablesz'
        else:
            print('allowed only:sh sz')
        
        sq="SELECT trade_date FROM tabledate"
#         try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listDate=[res[0] for res in result]
#         print(listDate)

        sq="SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name='"\
        +str(tableOut)+"' AND table_schema='nxstock'"
#         print(sq)
#             try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listCol=[res[0] for res in result]
#         print(listCol)
        for iDate in listDate:
            if not str(iDate) in listCol:
                sq="ALTER TABLE %s ADD COLUMN `%s` VARCHAR(4)"%(tableOut,iDate)
#                 print(sq)
                self.__cursor.execute(sq)
        
        
        sq="SELECT stock_code FROM "+str(tableHs)
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listHs=[res[0] for res in result]
        print(listHs)
        for codeHs in listHs:     
            sq="INSERT INTO "+tableOut+"(stock_code) SELECT '"+str(codeHs)+"' FROM dual "\
                +"WHERE NOT EXISTS(SELECT stock_code FROM "+tableOut+" WHERE stock_code="+str(codeHs)+")"                
            print(sq)
            self.__cursor.execute(sq)
#             except:
#                 print("error read COLUMNS")
#         except:
#             print("error read tabledate")
###########################################################################################################
    def createTableZs(self,zsCode): 
        sq="CREATE TABLE IF NOT EXISTS `tablezs%s`(id INT NOT NULL AUTO_INCREMENT,trade_date INT,`open` DECIMAL(8,2),"%zsCode\
            +"`close` DECIMAL(8,2),`change` DECIMAL(8,2),`percent` DECIMAL(6,2),`low` DECIMAL(8,2),`high` DECIMAL(8,2),"\
            +"volume BIGINT,amount DECIMAL(12,2),turnover DECIMAL(6,2),PRIMARY KEY(id),UNIQUE(trade_date))"
        print(sq)
        self.__cursor.execute(sq)   
