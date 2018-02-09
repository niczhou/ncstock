#coding=utf-8
import pymysql
import time

class HqUtil:  
      
    def indexOfList(self,element,mArray):
        mList=None
        if type(mArray)=='tuple':
            mList=list(mArray)
        elif type(mArray)=='list':
            mList=mArray
                        
        if element in mList:
            mIndex=mList.index(element)
        else:
             mIndex=-1
             
        return mIndex
    
    def getEndDate(self,inputDate,connection):
        if isinstance(connection, pymysql.connections.Connection):
            cursor=connection.cursor()
            sq="SELECT MAX(trade_date) FROM tabledate WHERE trade_date<=" \
                 +str(inputDate)
            #    print(sq)
            try:
                cursor.execute(sq)
                result=cursor.fetchone()
                return result[0]
            except:
                return 0
    def getStartDate(self,inputDate,days,connection):
        if isinstance(connection, pymysql.connections.Connection):
            cursor=connection.cursor()
            sq="SELECT MIN(trade_date) FROM(SELECT trade_date FROM tabledate WHERE trade_date<=" \
                 +str(inputDate)+" ORDER BY trade_date DESC LIMIT "+str(days)+") AS startdate"
            ##    print(sq)
            try:
                cursor.execute(sq)
                result=cursor.fetchone()
                return result[0]
            except:
                return 0        

    