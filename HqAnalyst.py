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

  def getIsBuy(self,stockCode,inputDate,analDays):
    mUtil=HqUtil()
    startDate=mUtil.getStartDate(inputDate,analDays,self.__conn)
#     if self.getIsBuyByClose(stockCode,startDate,inputDate)==False:
#       return False
    if self.getIsBuyByAmount(stockCode,startDate,inputDate)==False:
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
    
    if minIndex/maxIndex<0.76:
        if minIndex/avgIndex<0.87:
#     double check with forward answer authority
            if self.getAdjustedRatioByClose(stockCode, startDate, endDate)<0.76:
                if self.getDateDiff(minDate,endDate)<4:
                    if self.getDateDiff(maxDate,minDate)>6:
                        isBuyByClose=True
    print(str(stockCode)+" clo:"+str(isBuyByClose)+" max:"+str(maxIndex)+"-"+str(maxDate)+"|min:"+str(minIndex)+"-"++"|avg:"+str(avgIndex))
    return isBuyByClose

  def getIsBuyByAmount(self,stockCode,startDate,endDate):
    isBuyByAmount=False
    index="amount"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    if minIndex/maxIndex<0.41:
      if minIndex/avgIndex<0.54:
        if self.getDateDiff(minDate,endDate)<4:
          if self.getDateDiff(maxDate,minDate)>6:
            isBuyByClose=True
    print(str(stockCode)+" amo:"+str(isBuyByAmount)+" max:"+str(maxIndex)+"-"+str(maxDate)+"|min:"+str(minIndex)+"-"+str(minDate)+"|avg:"+str(avgIndex))
    return isBuyByAmount
	
  def getMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT MIN(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]

  def getMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT MAX(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]
  
  def getAvgByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT AVG(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]
    
  def getDateByIndexValue(self,stockCode,stockIndex,stockValue):
    self.__sq="SELECT trade_date"+" FROM `"+str(stockCode)+"` WHERE `"+str(stockIndex)+"`='"+str(stockValue)+"'"
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    return result[0]

  def getDateDiff(self,firstDate,secondDate):
    self.__sq="SELECT MIN(ID) FROM listdate"+" WHERE trade_date>="+str(firstDate)
    self.__cursor.execute(self.__sq)
    firstdateId=self.__cursor.fetchone()
    
    self.__sq="SELECT MAX(ID) FROM listdate"+" WHERE trade_date<="+str(secondDate)
    self.__cursor.execute(self.__sq)
    seconddateId=self.__cursor.fetchone()    
    
    dateDiff=seconddateId[0]-firstdateId[0]
    return dateDiff
  
  def getAdjustedRatioByClose(self,stockCode,startDate,endDate):
        self.__sq="SELECT `percent` FROM `"+stockCode+"` WHERE trade_date<="+endDate \
          +" AND trade_date>"+startDate+" ORDER BY `percent` DESC"
        self.__cursor.execute(self.__sq)
        percArr=self.__cursor.fetchall()
        ratio=1
        if percArr!=None:
            for i in range(0,len(percArr)):
#                 print(str(ratio)+"|"+str(percArr[i][0]))
                ratio=ratio*(100-percArr[i][0])/100
                       
        return ratio   
                
      
