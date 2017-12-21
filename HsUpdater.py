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
        rowCount=mSheet.nrows

        for i in range(1,rowCount+1):
#             sql="INSERT INTO listsz(stock_code,stock_name,stock_ipo,stock_total,stock_circulation) VALUES(" \
#             +mSheet.cell(i,5).value +","+mSheet.cell(i,6).value+","+mSheet.cell(i,7).value \
#             +","+mSheet.cell(i,8).value +","+mSheet.cell(i,9).value+")"
            sql="SELECT stock_code FROM listsz WHERE stock_code='"+mSheet.cell(i,5).value+"'"
            self.__cursor.execute(sql)
            result=self.__cursor.fetchone()
            
            if result==None:
                sql="INSERT INTO listsz (stock_code) VALUES ('" \
                +mSheet.cell(i,5).value+"')"
                print(str(i)+":"+sql)
                self.__cursor.execute(sql)
    
