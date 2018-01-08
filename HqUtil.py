#coding=utf-8
import pymysql

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
    
    def getStartDate(self,inputDate,days,connection):
        if isinstance(connection, pymsql.connections.connect):
            sq="SELECT MIN(trade_date) FROM(SELECT trade_date FROM listdate WHERE trade_date<" \
                 +str(inputDate)+" ORDER BY trade_date DESC LIMIT "+str(days)+") AS startdate"
            ##    print(sq)
            cursor=connection.cursor()
            cursor.execute(sq)
            result=cursor.fetchone()
            return result[0]
        
    def ifRecordDa(self,connection):
        if isinstance(connection, pymysql.connections.Connection):
            cursor=connection.cursor()
            sq="select 1 from listsh where stock_code = stock_code limit 1"
            cursor.execute(sq)
            result=cursor.fetchone()
            print(result)
    