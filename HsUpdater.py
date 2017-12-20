#coding=utf-8

import xlrd
import pymysql


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
        for i in range(1,11):
            sql="INSERT INTO listsz(stock_code,stock_name,stock_ipo,stock_total,stock_circulation) VALUES(" \
            +mSheet.cell(i,5).value +","+mSheet.cell(i,6).value +","+mSheet.cell(i,7).value \
            +","+mSheet.cell(i,8).value +","+mSheet.cell(i,9).value +")"
            print(sql)
#             self.__cursor.execute(sql)