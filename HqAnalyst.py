#coding=utf-8
import threadpool
import pymysql
import time
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
  def ifBuyTodayByHs(self,tableHs,endDate,HqStrategy=0):
    if HqStrategy!=0 and HqStrategy!=1:
        print("invlida HqStrategy:")
        return 
    
    sq="SELECT stock_code FROM %s"%tableHs
#     try:
    self.__cursor.execute(sq)
    result=self.__cursor.fetchall()  
    listHs=[result[i][0] for i in range(len(result))]
    
#     listParas=[([codeHs,endDate,HqStrategy],None) for codeHs in listHs]
#     pool=threadpool.ThreadPool(4)
#     requests=threadpool.makeRequests(self.ifBuyByDate,listParas)
#     [pool.putRequest(req) for req in requests]
#     pool.wait()
    for codeHs in listHs:
        self.ifBuyByDate(codeHs, endDate,HqStrategy)
#     except:
#         print("analyze fail")
####################################################################################
  def ifBuyAlltimeByHs(self,tableHs,HqStrategy=0):
    if HqStrategy!=0 and HqStrategy!=1:
        print("HqStrategy allows only 0 or 1")
        return 
    
    sq="SELECT stock_code FROM %s"%tableHs
    self.__cursor.execute(sq)
    result=self.__cursor.fetchall()  
    listHs=[result[i][0] for i in range(len(result))]
    for codeHs in listHs:
        self.ifBuyByCode(codeHs,HqStrategy)
####################################################################################      
  def ifBuyByCode(self,stockCode,HqStrategy=0):     
    sq="SELECT trade_date FROM `%s` LIMIT 21"%stockCode
    self.__cursor.execute(sq)
    result=self.__cursor.fetchall()
    listDate=[res[0] for res in result]
    for dt in listDate[::-1]:
        self.ifBuyByDate(stockCode,dt,HqStrategy)
        
 #############################################################################################
  def ifBuyByDate(self,stockCode,endDate,HqStrategy=0):
    if HqStrategy==0:
        days=11
    elif HqStrategy==1:
        days=29
    mUtil=HqUtil()
    isBuy=False
#     endDate=mUtil.getEndDate(endDate,self.__conn)
    startDate=mUtil.getStartDate(endDate,days,self.__conn)
    if self.ifBuyByClose(stockCode, startDate, endDate,HqStrategy)==True:
        if self.ifBuyByAmount(stockCode, startDate, endDate,HqStrategy)==True:
            isBuy=True 
    if isBuy==True:
        print(str(stockCode)+" buy at "+str(endDate) )           
    return isBuy       
    
#####close#######################close###########################close######################
  def ifBuyByClose(self,stockCode,startDate,endDate,HqStrategy=0):
    if HqStrategy==0:
        valMinMax=0.88
        valMinAvg=0.92
        valDateDiffMinEnd=4
        valDateDiffMaxMin=6
    elif HqStrategy==1:
        valMinMax=0.78
        valMinAvg=0.88
        valDateDiffMinEnd=4
        valDateDiffMaxMin=16
        
    index="close"
    isBuy=False
    maxIndex=minIndex=avgIndex=0.00
    dateMaxIndex=minDate=0   
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
        if minMax<valMinMax:
            if avgIndex:
                minAvg=minIndex/avgIndex
                if minAvg<valMinAvg:
        #     double check with forward answer authority
                    aRatio=self.getAdjustedRatio(stockCode, startDate, endDate)
                    if aRatio<valMinMax:
                        minEndDiff=self.dateDiff(stockCode,minDate,endDate)
                        if minEndDiff<valDateDiffMinEnd:
                            maxMinDiff=self.dateDiff(stockCode,maxDate,minDate)
                            if maxMinDiff>valDateDiffMaxMin:
                                isBuy=True
    if isBuy==True:
        print(str(stockCode)+"-"+str(startDate)+"-"+str(endDate)+"\tclo:"+str(isBuy)+"\tmax:"+str(maxDate)+"-"+str(round(maxIndex,2)) \
          +"\tmin:"+str(minDate)+"-"+str(minIndex)+"\tavg:"+str(avgIndex) \
          +"\tm/m:"+str(round(minMax,3))+"\tm/a:"+str(round(minAvg,3))+"\tratio:" \
          +str(round(aRatio,3))+"\tmeDiff:"+str(minEndDiff)+"\tmmDiff:"+str(maxMinDiff))
             
    return isBuy     

#####amount---------------------------amount--------------------------amount------------------------------
  def ifBuyByAmount(self,stockCode,startDate,endDate,HqStrategy=0):
    if HqStrategy==0:
        valMinMax=0.39
        valMinAvg=0.51
        valDateDiffMinEnd=4
        valDateDiffMaxMin=6
    elif HqStrategy==1:
        valMinMax=0.37
        valMinAvg=0.57
        valDateDiffMinEnd=6
        valDateDiffMaxMin=16
    
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
        if minMax<valMinMax:
            if avgIndex:
                minAvg=minIndex/avgIndex
                if minAvg<valMinAvg:
        #     double check with forward answer authority
#                     aRatio=self.getAdjustedRatioByClose(stockCode, startDate, endDate)
#                     if aRatio<0.78:
                    minEndDiff=self.dateDiff(stockCode,minDate,endDate)
                    if minEndDiff<valDateDiffMinEnd:
                        maxMinDiff=self.dateDiff(stockCode,maxDate,minDate)
                        if maxMinDiff>valDateDiffMaxMin:
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
                
      
