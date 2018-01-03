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
#     print("__del__")
    pass

  def getIsBuy(self,stockCode,inputDate,analDays):
    mUtil=HqUtil()
    startDate=mUtil.getStartDate(inputDate,analDays,self.__conn)
    if self.getIsBuyByClose(stockCode,startDate,inputDate)==False:
      return False
#     elif self.getIsBuyByAmount(stockCode,startDate,inputDate)==False:
#       return False
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
    
    if minIndex!=0:
        minMax=minIndex/maxIndex
        if minMax<0.78:
            if avgIndex!=0:
                minAvg=minIndex/avgIndex
                if minAvg<0.92:
        #     double check with forward answer authority
                    aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
                    if aRatio<0.78:
                        minEndDiff=self.getDateDiff(minDate,endDate)
                        if minEndDiff<3:
                            maxMinDiff=self.getDateDiff(maxDate,minDate)
                            if maxMinDiff>6:
                                isBuyByClose=True
    if isBuyByClose==True:
        print(str(stockCode)+" clo:"+str(isBuyByClose)+" max:"+str(maxIndex)+"-"+str(maxDate)+"|min:"+str(minIndex)+"-"+str(minDate)+"|avg:"+str(avgIndex))
        print(str(stockCode)+" m/m:"+str(round(minMax,3))+" |m/a:"+str(round(minAvg,3)) \
          +" |ratio:"+str(round(aRatio,3))+" |meDiff:"+str(minEndDiff)+"|mmDiff:"+str(maxMinDiff))
    
    return isBuyByClose      


  def getIsBuyByAmount(self,stockCode,startDate,endDate):
    isBuyByAmount=False
    index="amount"
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    if minIndex!=0:
        minMax=minIndex/maxIndex
        if minMax<0.38:
            if avgIndex!=0:
                minAvg=minIndex/avgIndex
                if minAvg<0.52:
        #     double check with forward answer authority
#                     aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
#                     if aRatio<0.78:
                    minEndDiff=self.getDateDiff(minDate,endDate)
                    if minEndDiff<3:
                        maxMinDiff=self.getDateDiff(maxDate,minDate)
                        if maxMinDiff>6:
                            isBuyByAmount=True
#     if isBuyByClose==True:
    print(str(stockCode)+" amo:"+str(isBuyByClose)+" max:"+str(maxIndex)+"-"+str(maxDate)+"|min:"+str(minIndex)+"-"+str(minDate)+"|avg:"+str(avgIndex))
    print(str(stockCode)+" m/m:"+str(round(minMax,3))+" |m/a:"+str(round(minAvg,3)) \
      +" |meDiff:"+str(minEndDiff)+"|mmDiff:"+str(maxMinDiff))
    
    return isBuyByAmount     
	
  def getMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT MIN(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    if result!=None:
        return result[0]
    else:
        return 0

  def getMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT MAX(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    if result!=None:
        return result[0]
    else:
        return 0
  
  def getAvgByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT AVG(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
    if result!=None:
        return result[0]
    else:
        return 0
    
  def getDateByIndexValue(self,stockCode,stockIndex,stockValue):
    if stockValue!=None and stockValue!=0:  
        self.__sq="SELECT trade_date"+" FROM `"+str(stockCode)+"` WHERE `"+str(stockIndex)+"`='"+str(stockValue)+"'"
        self.__cursor.execute(self.__sq)
        result=self.__cursor.fetchone()
        if result!=None:
            return result[0]
        else:
            return 0

  def getDateDiff(self,firstDate,secondDate):
    self.__sq="SELECT MIN(ID) FROM listdate"+" WHERE trade_date>="+str(firstDate)
    self.__cursor.execute(self.__sq)
    firstdateId=self.__cursor.fetchone()
    
    self.__sq="SELECT MAX(ID) FROM listdate"+" WHERE trade_date<="+str(secondDate)
    self.__cursor.execute(self.__sq)
    seconddateId=self.__cursor.fetchone()    
    
    dateDiff=seconddateId[0]-firstdateId[0]
#     print(str(dateDiff))
    return dateDiff
  
  def getAdjustedRatioByClose(self,stockCode,startDate,endDate):
        self.__sq="SELECT `percent` FROM `"+stockCode+"` WHERE trade_date<="+endDate \
          +" AND trade_date>"+startDate+" ORDER BY `percent` DESC"
        self.__cursor.execute(self.__sq)
        listPerc=self.__cursor.fetchall()
        ratio=1
        if listPerc!=None:
            for i in range(0,len(listPerc)):
#                 print(str(ratio)+"|"+str(listPerc[i][0]))
                ratio=ratio*(100-listPerc[i][0])/100
                       
        return ratio   
                
      
