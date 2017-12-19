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

    if self.getIsBuyByClose(stockCode,startDate,endDate)=="false":
      return "false"
    elif self.getIsBuyByAmount(stockCode,startDate,endDate)=="false":
      return "fasle"
    else:
      return "true"

  def getIsBuyByClose(self,stockCode,startDate,endDate):
    isBuyByClose="false"
    index="close"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)

##    print(avgIndex)
    
    if minIndex/maxIndex<0.72:
      if minIndex/avgIndex<0.85:
        if self.getDateDiff(minDate,endDate)<5:
          if self.getDateDiff(maxDate,minDate)>15:
            isBuyByClose=true

    return isBuyByClose

  def getIsBuyByAmount(self,stockCode,startDate,endDate):
    isBuyByAmount="false"
    index="amount"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)

##    print(avgIndex)
    
    if minIndex/maxIndex<0.33:
      if minIndex/avgIndex<0.42:
        if self.getDateDiff(minDate,endDate)<5:
          if self.getDateDiff(maxDate,minDate)>15:
            isBuyByClose=true

    return isBuyByAmount
	
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
    self.__sql="SELECT AVG("+str(stockIndex)+") FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
##    print(self.__sql)
    self.__cursor.execute(self.__sql)
    result=self.__cursor.fetchone()
    return result[0]
    
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
  
