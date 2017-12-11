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


  def getIsBuyByCode(self,stockCode,startDate,endDate):
    indexTup=("close","amount")
    targetTup=(0.75,0.45)
    for index in indexTup:
##      print(index)
      self.getMinMaxByIndex(stockCode,index,startDate,endDate)

  def getIsBuyByClose(self,stockCode,startDate,endDate):
    isBuyByClose="false"
    index="close"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)   
    
    if minIndex/maxIndex<0.72:
      if self.getDateDiff(maxDate,minDate)>15:
        if self.getDateDiff(minDate,endDate)<5:
          isBuyByClose=true

    return isBuyByClose

  def getIsBuyByAmount(self,stockCode,startDate,endDate):
    isBuyByClose="false"
    index="amount"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    mmIndex=minIndex/maxIndex
    mmDiff=self.getDateDiff(maxDate,minDate)
    meDiff=self.getDateDiff(minDate,endDate)
    print(maxIndex)
    print(maxDate)
    print(minIndex)
    print(minDate)
    print(mmDiff)
    
    if mmDiff>15:
      if meDiff<5:
        if mmIndex<0.72:
          isBuyByClose=true

    return isBuyByClose
	
  def getMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sql="SELECT MIN("+str(stockIndex)+") FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    result=self.__cursor.fetchone()
##    print(result)
    return result[0]

  def getMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sql="SELECT MAX("+str(stockIndex)+") FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    result=self.__cursor.fetchone()
##    print(result)
    return result[0]
  
  def getAvgByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sql="SELECT AVG(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
##    print(minIndex/maxIndex)
    return minIndex/maxIndex
    
  def getDateByIndexValue(self,stockCode,stockIndex,stockValue):
    self.__sql="SELECT trade_date"+" FROM `"+str(stockCode)+"` WHERE "+str(stockIndex)+"='"+str(stockValue)+"'"
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    result=self.__cursor.fetchone()
##    print(result)
    return result[0]

  def getDateDiff(self,firstDate,secondDate):
    self.__sql="SELECT MIN(ID) FROM listdate"+" WHERE trade_date>="+str(firstDate)
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    firstdateId=self.__cursor.fetchone()
##    print(firstdateId[0])
    
    self.__sql="SELECT MAX(ID) FROM listdate"+" WHERE trade_date<="+str(secondDate)
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    seconddateId=self.__cursor.fetchone()
##    print(seconddateId[0])    
    
    dateDiff=seconddateId[0]-firstdateId[0]
    return dateDiff
  
