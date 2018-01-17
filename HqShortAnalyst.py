#coding=utf-8
import threadpool
import pymysql
import time
from HqUtil import HqUtil

class HqShortAnalyst:
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
  def ifBuyByHs(self,tableHs,endDate):
    days=11
    mUtil=HqUtil()
    sq="SELECT stock_code FROM %s"%tableHs
#     try:
    self.__cursor.execute(sq)
    result=self.__cursor.fetchall()  
    listHs=[result[i][0] for i in range(len(result))]
#         listParas=[([codeHs,endDate,days],None) for codeHs in listHs]
#         pool=threadpool.ThreadPool(4)
#         requests=threadpool.makeRequests(self.ifBuyByDate,listParas)
#         [pool.putRequest(req) for req in requests]
#         pool.wait()
    for codeHs in listHs:
        self.ifBuyByDate(codeHs, endDate, days)
#     except:
#         print("analyze fail %s"%codeHs)
      
  def ifBuyByCode(self,stockCode):
    sq="SELECT trade_date FROM `%s`"%stockCode
    self.__cursor.execute(sq)
    result=self.__cursor.fetchall()
    listDate=[res[0] for res in result]
    for dt in listDate[::-1]:
        self.ifBuyByDate(stockCode,dt,11)
        
 #############################################################################################
  def ifBuyByDate(self,stockCode,endDate,days):
    mUtil=HqUtil()
    isBuy=False
    startDate=mUtil.getStartDate(endDate,days,self.__conn)
    if self.ifBuyByClose(stockCode, startDate, endDate)==True:
        if self.ifBuyByAmount(stockCode, startDate, endDate)==True:
            isBuy=True 
    if isBuy==True:
        print(str(stockCode)+" buy at "+str(endDate) )           
    return isBuy       
    
#####close#######################close###########################close######################
  def ifBuyByClose(self,stockCode,startDate,endDate):
    index="close"
    isBuy=False
    maxIndex=minIndex=avgIndex=0.00
    dateMaxClose=minDate=0   
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateMaxByIndex(stockCode,index,startDate,endDate)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateMinByIndex(stockCode,index,startDate,endDate)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)
    
    minMax=minAvg=aRatio=0.0
    minEndDiff=maxMinDiff=0
#     print(self.getMaxByIndex(stockCode,index,startDate,endDate))
    if maxIndex:
        minMax=minIndex/maxIndex
        if minMax<0.88:
            if avgIndex:
                minAvg=minIndex/avgIndex
                if minAvg<0.92:
        #     double check with forward answer authority
                    aRatio=self.getAdjustedRatio(stockCode, startDate, endDate)
                    if aRatio<0.88:
                        minEndDiff=self.dateDiff(stockCode,minDate,endDate)
                        if minEndDiff<4:
                            maxMinDiff=self.dateDiff(stockCode,maxDate,minDate)
                            if maxMinDiff>6:
                                isBuy=True
    if isBuy==True:
        print(str(stockCode)+"-"+str(startDate)+"-"+str(endDate)+"\tclo:"+str(isBuy)+"\tmax:"+str(maxDate)+"-"+str(round(maxIndex,2)) \
          +"\tmin:"+str(minDate)+"-"+str(minIndex)+"\tavg:"+str(avgIndex) \
          +"\tm/m:"+str(round(minMax,3))+"\tm/a:"+str(round(minAvg,3))+"\tratio:" \
          +str(round(aRatio,3))+"\tmeDiff:"+str(minEndDiff)+"\tmmDiff:"+str(maxMinDiff))
             
    return isBuy     

#####amount---------------------------amount--------------------------amount------------------------------
  def ifBuyByAmount(self,stockCode,startDate,endDate):
    index="amount"
    isBuy=False
    maxIndex=minIndex=avgIndex=0.00
    maxDate=minDate=0   
    maxIndex=self.getMaxByIndex(stockCode,index,startDate,endDate)
    maxDate=self.getDateMaxByIndex(stockCode,index,startDate,endDate)
    minIndex=self.getMinByIndex(stockCode,index,startDate,endDate)
    minDate=self.getDateMinByIndex(stockCode,index,startDate,endDate)
    avgIndex=self.getAvgByIndex(stockCode,index,startDate,endDate)

    minMax=minAvg=aRatio=0.0
    minEndDiff=maxMinDiff=0
    if maxIndex:
        minMax=minIndex/maxIndex
        if minMax<0.38:
            if avgIndex:
                minAvg=minIndex/avgIndex
                if minAvg<0.42:
        #     double check with forward answer authority
#                     aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
#                     if aRatio<0.78:
                    minEndDiff=self.dateDiff(stockCode,minDate,endDate)
                    if minEndDiff<5:
                        maxMinDiff=self.dateDiff(stockCode,maxDate,minDate)
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
    sq="SELECT `%s` FROM (SELECT trade_date,`%s` FROM `%s` WHERE trade_date>=%d AND trade_date<=%d ORDER BY `%s` DESC LIMIT 1) AS mt"\
        %(stockIndex,stockIndex,stockCode,startDate,endDate,stockIndex)
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
    sq="SELECT `%s` FROM (SELECT trade_date,`%s` FROM `%s` WHERE trade_date>=%d AND trade_date<=%d ORDER BY `%s` LIMIT 1) AS mt"\
        %(stockIndex,stockIndex,stockCode,startDate,endDate,stockIndex)
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
    
  def getDateMaxByIndex(self,stockCode,stockIndex,startDate,endDate):
    sq="SELECT trade_date FROM (SELECT trade_date,`%s` FROM `%s` WHERE trade_date>=%d AND trade_date<=%d ORDER BY `%s` DESC LIMIT 1) AS mt"\
        %(stockIndex,stockCode,startDate,endDate,stockIndex)
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
  def getDateMinByIndex(self,stockCode,stockIndex,startDate,endDate):
    sq="SELECT trade_date FROM (SELECT trade_date,`%s` FROM `%s` WHERE trade_date>=%d AND trade_date<=%d ORDER BY `%s` LIMIT 1) AS mt"\
        %(stockIndex,stockCode,startDate,endDate,stockIndex)
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

  def dateDiff(self,stockCode,firstDate,secondDate):
    sq="SELECT COUNT(trade_date) FROM `%s` WHERE trade_date>=%d and trade_date<=%d"\
        %(stockCode,firstDate,secondDate)
    try:
        self.__cursor.execute(sq)
        result=self.__cursor.fetchone()
        if result:
            if result[0]:
                return result[0]
            else:
                return 0
        else:
            return 0
    except:
        return 0
  
  def getAdjustedRatio(self,stockCode,startDate,endDate):
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
                
      
