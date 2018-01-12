#coding=utf-8
import threadpool
import pymysql
from HqUtil import HqUtil

class HqAnalyst:
  __conn=None
  __cursor=None

  def __init__(self,connection):
#     print('__init__')
    self.__conn=connection
    self.__cursor=connection.cursor()
    
  def __del__(self):
    pass
#     print("__del__")
###########################################################################################
  def getIsBuyByHs(self,tableHs,inputDate,analDays):
    mUtil=HqUtil()
    sq="SELECT stock_code FROM tablesz LIMIT 200"
    try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()  
        listSz=[result[i][0] for i in range(len(result))]
        startDate=mUtil.getStartDate(inputDate,analDays,self.__conn)
        listParas=[([codeSz,startDate,inputDate],None) for codeSz in listSz]
    #     print(listParas)  
        pool=threadpool.ThreadPool(2)
        requests=threadpool.makeRequests(self.getIsBuyByCode,listParas)
        [pool.putRequest(req) for req in requests]
        pool.wait()
    except:
        print("analyze fail %s"%stockCode)
      
  def getIsBuyByCode(self,stockCode,startDate,endDate):
    if self.getIsBuyByClose(stockCode,startDate,endDate)==False:
      return False
    elif self.getIsBuyByAmount(stockCode,startDate,endDate)==False:
      return False
    else:
      return True
#####close#######################close###########################close######################
  def getIsBuyByClose(self,stockCode,startDate,endDate):
    index="close"
    isBuy=False
    maxIndex=minIndex=avgIndex=0.00
    maxDate=minDate=0   
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex,startDate,endDate)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex,startDate,endDate)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    minMax=minAvg=aRatio=0.0
    minEndDiff=maxMinDiff=0
#     print(self.getMaxByIndex(stockCode,index,startDate,endDate))
    if maxIndex:
        minMax=minIndex/maxIndex
        if minMax<0.81:
            if avgIndex!=0:
                minAvg=minIndex/avgIndex
                if minAvg<0.87:
        #     double check with forward answer authority
                    aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
                    if aRatio<0.81:
                        minEndDiff=self.getDateDiff(minDate,endDate)
                        if minEndDiff<5:
                            maxMinDiff=self.getDateDiff(maxDate,minDate)
                            if maxMinDiff>6:
                                isBuy=True
    if isBuy==True:
        print(str(stockCode)+"-"+str(startDate)+"-"+str(endDate)+"\tclo:"+str(isBuy)+"\tmax:"+str(maxDate)+"-"+str(round(maxIndex,2)) \
          +"\tmin:"+str(minDate)+"-"+str(minIndex)+"\tavg:"+str(avgIndex) \
          +"\tm/m:"+str(round(minMax,3))+"\tm/a:"+str(round(minAvg,3))+"\tratio:" \
          +str(round(aRatio,3))+"\tmeDiff:"+str(minEndDiff)+"\tmmDiff:"+str(maxMinDiff))
             
    return isBuy     

#####amount---------------------------amount--------------------------amount------------------------------
  def getIsBuyByAmount(self,stockCode,startDate,endDate):
    index="amount"
    isBuy=False
    maxIndex=minIndex=avgIndex=0.00
    maxDate=minDate=0
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateByIndexValue(stockCode,index,maxIndex,startDate,endDate)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateByIndexValue(stockCode,index,minIndex,startDate,endDate)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)

    minMax=minAvg=aRatio=0.0
    minEndDiff=maxMinDiff=0
    if maxIndex:
        minMax=minIndex/maxIndex
        if minMax<0.23:
            if avgIndex:
                minAvg=minIndex/avgIndex
                if minAvg<0.37:
        #     double check with forward answer authority
#                     aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
#                     if aRatio<0.78:
                    minEndDiff=self.getDateDiff(minDate,endDate)
                    if minEndDiff<5:
                        maxMinDiff=self.getDateDiff(maxDate,minDate)
                        if maxMinDiff>10:
                            isBuy=True
    if isBuy==True:
        print(str(stockCode)+"-"+str(startDate)+"-"+str(endDate)+"\tamo:"+str(isBuy)+"\tmax:"+str(maxDate)+"-"+str(maxIndex) \
          +"\tmin:"+str(minDate)+"-"+str(minIndex)+"\tavg:"+str(avgIndex) \
          +"\tm/m:"+str(round(minMax,3))+"\tm/a:"+str(round(minAvg,3))+"\tratio:" \
          +str(round(aRatio,3))+"\tmeDiff:"+str(minEndDiff)+"\tmmDiff:"+str(maxMinDiff))
    
    return isBuy     
######################amount################################amount###########################	
  def getMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    sq="SELECT MAX(`%s`) FROM `%s` WHERE trade_date>=%d AND trade_date<=%d"\
        %(stockIndex,stockCode,startDate,endDate)
#     print(sq)
    try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchone()
        if result:
            if result[0]:
#                 print(stockCode+stockIndex+' max:'+str(result[0])) 
                return result[0]
            else:
                return 0
        else:
            return 0
    except:
        return 0
    
  def getMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    sq="SELECT MIN(`%s`) FROM `%s` WHERE trade_date>=%d AND trade_date<=%d"\
        %(stockIndex,stockCode,startDate,endDate)
    try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchone()
        if result:
            if result[0]:
#                 print(stockCode+stockIndex+' min:'+str(result[0])) 
                return result[0]
            else:
                return 0
        else:
            return 0
    except:
        return 0

  def getAvgByIndex(self,stockCode,stockIndex,startDate,endDate):
    sq="SELECT AVG(`%s`) FROM `%s` WHERE trade_date>=%d AND trade_date<=%d"\
        %(stockIndex,stockCode,startDate,endDate)
    try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchone()
        if result:
            if result[0]:
#                 print(stockCode+stockIndex+' avg:'+str(round(result[0],2))) 
                return round(result[0],2)
            else:
                return 0
        else:
            return 0
    except:
        return 0
    
  def getDateByIndexValue(self,stockCode,stockIndex,stockValue,startDate,endDate):
    if stockValue:  
        sq="SELECT trade_date FROM `%s` WHERE `%s`=%r AND trade_date>=%d AND trade_date<=%d"\
            %(stockCode,stockIndex,str(stockValue),startDate,endDate)
    try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchone()
    #     print(stockCode+stockIndex+' min:'+str(result[0]))    
        return int(result[0])
    except:
        return 0

  def getDateDiff(self,firstDate,secondDate):
    sq="SELECT MIN(id) FROM tabledate"+" WHERE trade_date>="+str(firstDate)
    try:
        self.__cursor.execute(sq)
        firstdateId=self.__cursor.fetchone()[0]
    except:
        return 0
    
    sq="SELECT MAX(ID) FROM tabledate"+" WHERE trade_date<="+str(secondDate)
    try:
        self.__cursor.execute(sq)
        seconddateId=self.__cursor.fetchone()[0]
    except:
        return 0    
    if firstdateId and seconddateId:
        dateDiff=seconddateId-firstdateId
#     print(str(dateDiff))
        return dateDiff
    else:
        return 0
  
  def getAdjustedRatioByClose(self,stockCode,startDate,endDate):
        sq="SELECT `percent` FROM `"+str(stockCode)+"` WHERE trade_date<="+str(endDate) \
          +" AND trade_date>"+str(startDate)+" ORDER BY `percent` DESC"
        try:
            self.__cursor.execute(sq)
            result=self.__cursor.fetchall()
            listPerc=[result[i][0] for i in range(len(result))]
            ratio=1
            if listPerc:
                for j in listPerc:
    #                 print(str(ratio)+"|"+str(listPerc[i][0]))
                    ratio=ratio*(100-listPerc)/100
                           
            return ratio
        except:
            return 0 
                
      
