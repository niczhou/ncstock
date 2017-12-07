#coding=utf-8
import pymysql

class HqAnalyst:
  __conn=None
  __cursor=None
  __sql=''

  def __init__(self,connection):
    print('__init__')
    self.__conn=connection
    self.__cursor=connection.cursor()
    
  def __del__(self):
    print("__del__")
##    self.__cursor.close()
##    self.__conn.close()
	
  def getMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sql="SELECT MIN("+stockIndex+") FROM `"+stockCode+"` WHERE trade_date>"+startDate+" AND trade_date<"+endDate
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    result=self.__cursor.fetchone()
    print(result)
    return result[0]

  def getMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sql="SELECT MAX("+stockIndex+") FROM `"+stockCode+"` WHERE trade_date>"+startDate+" AND trade_date<"+endDate
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    result=self.__cursor.fetchone()
    print(result)
    return result[0]  
    
  def getDateByIndexValue(self,stockCode,stockIndex,stockValue):
    self.__sql="SELECT trade_date"+" FROM `"+stockCode+"` WHERE "+stockIndex+"='"+stockValue+"'"
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    result=self.__cursor.fetchone()
    print(result)
    return result[0]

  def getMinMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    minIndex=self.getMinByIndex(stockCode,stockIndex,startDate,endDate)
    maxIndex=self.getMaxByIndex(stockCode,stockIndex,startDate,endDate)
    print(minIndex/maxIndex)
    return minIndex/maxIndex

  def getDateDiff(self,firstDate,secondDate):
    self.__sql="SELECT MIN(ID) FROM listdate"+" WHERE trade_date>="+firstDate
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    firstdateId=self.__cursor.fetchone()
    print(firstdateId[0])
    
    self.__sql="SELECT MAX(ID) FROM listdate"+" WHERE trade_date<="+secondDate
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    seconddateId=self.__cursor.fetchone()
    print(seconddateId[0])    
    
    dateDiff=seconddateId[0]-firstdateId[0]
    return dateDiff
