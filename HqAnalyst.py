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
#####close#######################close###########################close######################
  def getIsBuyByClose(self,stockCode,startDate,endDate):
    index="close"
    isBuy=False
    maxIndex=minIndex=avgIndex=0.00
    maxDate=minDate='19500101'    
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    minMax=minAvg=aRatio=0.0
    minEndDiff=maxMinDiff=0
#     print(self.getMaxByIndex(stockCode,index,startDate,endDate))
    if maxIndex!=0:
        minMax=minIndex/maxIndex
        if minMax<0.81:
            if avgIndex!=0:
                minAvg=minIndex/avgIndex
                if minAvg<0.87:
        #     double check with forward answer authority
                    aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
                    if aRatio<0.81:
                        minEndDiff=self.getDateDiff(minDate,endDate)
                        if minEndDiff<4:
                            maxMinDiff=self.getDateDiff(maxDate,minDate)
                            if maxMinDiff>7:
                                isBuy=True
    if isBuy==True:
        print(str(stockCode)+"\tclo:"+str(isBuy)+"\tmax:"+str(maxDate)+"-"+str(maxIndex) \
          +"\tmin:"+str(minDate)+"-"+str(minIndex)+"\tavg:"+str(avgIndex) \
          +"\tm/m:"+str(round(minMax,3))+"\tm/a:"+str(round(minAvg,3))+"\tratio:" \
          +str(round(aRatio,3))+"\tmeDiff:"+str(minEndDiff)+"\tmmDiff:"+str(maxMinDiff))
             
    return isBuy     

#####amount---------------------------amount--------------------------amount------------------------------
  def getIsBuyByAmount(self,stockCode,startDate,endDate):
    index="amount"
    isBuy=False
    maxIndex=minIndex=avgIndex=0.00
    maxDate=minDate='19500101'
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    minMax=minAvg=aRatio=0.0
    minEndDiff=maxMinDiff=0
    if maxIndex!=0:
        minMax=minIndex/maxIndex
        if minMax<0.21:
            if avgIndex!=0:
                minAvg=minIndex/avgIndex
                if minAvg<0.33:
        #     double check with forward answer authority
#                     aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
#                     if aRatio<0.78:
                    minEndDiff=self.getDateDiff(minDate,endDate)
                    if minEndDiff<4:
                        maxMinDiff=self.getDateDiff(maxDate,minDate)
                        if maxMinDiff>16:
                            isBuy=True
    if isBuy==True:
        print(str(stockCode)+"\tamo:"+str(isBuy)+"\tmax:"+str(maxDate)+"-"+str(maxIndex) \
          +"\tmin:"+str(minDate)+"-"+str(minIndex)+"\tavg:"+str(avgIndex) \
          +"\tm/m:"+str(round(minMax,3))+"\tm/a:"+str(round(minAvg,3))+"\tratio:" \
          +str(round(aRatio,3))+"\tmeDiff:"+str(minEndDiff)+"\tmmDiff:"+str(maxMinDiff))
    
    return isBuy     
#####################################################################################################	
  def getMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT MAX(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
#     print(stockCode+' max:'+str(result[0]))    
    if result[0]:
        return result[0]
    else:
        return 0.00
    
  def getMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT MIN(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
#     print(stockCode+' min:'+str(result[0]))    
    if result[0]:
        return result[0]
    else:
        return 0.00

  def getAvgByIndex(self,stockCode,stockIndex,startDate,endDate):
    self.__sq="SELECT AVG(`"+str(stockIndex)+"`) FROM `"+str(stockCode)+"` WHERE trade_date>"\
                +str(startDate)+" AND trade_date<"+str(endDate)
    self.__cursor.execute(self.__sq)
    result=self.__cursor.fetchone()
#     print(stockCode+' avg:'+str(result[0]))
    if result[0]:
        return round(result[0],2)
    else:
        return 0.00
    
  def getDateByIndexValue(self,stockCode,stockIndex,stockValue):
    if stockValue!=None and stockValue!=0:  
        self.__sq="SELECT trade_date"+" FROM `"+str(stockCode)+"` WHERE `"+str(stockIndex)+"`='"+str(stockValue)+"'"
        self.__cursor.execute(self.__sq)
        result=self.__cursor.fetchone()
        if result[0]:
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
                
      
