#coding=utf-8
import pymysql
from HqUtil import HqUtil

class HqAnalyst:
  __conn=None
  __cursor=None
  __sq=''

  def __init__(self,connection):
#     print('__init__')
    self.__conn=connection
    self.__cursor=connection.cursor()
    
  def __del__(self):
    print("__del__")

  def getIsBuyByCode(self,stockCode,inputDate,analDays):
    mUtil=HqUtil()
    startDate=mUtil.getStartDate(inputDate,analDays,self.__conn)
    if self.getIsBuyByClose(stockCode,startDate,inputDate)==False:
      return False
    elif self.getIsBuyByAmount(stockCode,startDate,inputDate)==False:
      return False
    else:
      return True

  def getIsBuyByClose(self,stockCode,startDate,endDate):
    isBuyByClose=False
    index="close"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    if minIndex/maxIndex<2:
      if minIndex/avgIndex<2:
        if self.getDateDiff(minDate,endDate)<111:
          if self.getDateDiff(maxDate,minDate)>0:
            isBuyByClose=True
    print(str(stockCode)+" clo:"+isBuyByClose+" max:"+str(maxIndex)+"-"+str(maxDate)+"|min:"+str(minIndex)+"-"+str(minDate)+"|avg:"+str(avgIndex))
    return isBuyByClose

  def getIsBuyByAmount(self,stockCode,startDate,endDate):
    isBuyByAmount=False
    index="amount"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    if minIndex/maxIndex<1:
      if minIndex/avgIndex<1:
        if self.getDateDiff(minDate,endDate)<111:
          if self.getDateDiff(maxDate,minDate)>0:
            isBuyByClose=True
    print(str(stockCode)+" amo:"+isBuyByAmount+" max:"+str(maxIndex)+"-"+str(maxDate)+"|min:"+str(minIndex)+"-"+str(minDate)+"|avg:"+str(avgIndex))
    return isBuyByAmount
	
  def getMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq=concat("SELECT MIN(",str(stockIndex),") FROM `",str(stockCode),"` WHERE trade_date>"\
                ,str(startDate)," AND trade_date<",str(endDate))
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]

  def getMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq=concat("SELECT MAX(",str(stockIndex),") FROM `",str(stockCode),"` WHERE trade_date>"\
                ,str(startDate)," AND trade_date<",str(endDate))
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]
  
  def getAvgByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq=concat("SELECT AVG(",str(stockIndex),") FROM `",str(stockCode),"` WHERE trade_date>"\
                ,str(startDate)," AND trade_date<",str(endDate))
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]
    
  def getDateByIndexValue(self,stockCode,stockIndex,stockValue):
    self.__sq=concat("SELECT trade_date"," FROM `",str(stockCode),"` WHERE ",str(stockIndex),"='",str(stockValue),"'")
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]

  def getDateDiff(self,firstDate,secondDate):
    self.__sq=concat("SELECT MIN(ID) FROM listdate"," WHERE trade_date>=",str(firstDate))
    self.__cursor.execute(self.__sq)
    firstdateId=self.__cursor.fetchone()
    
    self.__sq=concat("SELECT MAX(ID) FROM listdate"," WHERE trade_date<=",str(secondDate))
    self.__cursor.execute(self.__sq)
    seconddateId=self.__cursor.fetchone()    
    
    dateDiff=seconddateId[0]-firstdateId[0]
    return dateDiff
  
