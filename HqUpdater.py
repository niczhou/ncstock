#coding=utf-8
import json
import time
import urllib.request
import urllib.error
import threadpool

class HqUpdater:
      
# 'url            http://q.stock.sohu.com/hisHq?code=cn_300228&start=20170413&end=20170417
# 'return         [{"Status":0,"hq":[["2017-04-27","15.46","15.46","1.41","10.04%","14.89","15.46","1460797","222747.31","13.66%"],
# '               ["2017-04-26","14.05","14.05","1.28","10.02%","14.05","14.05","29886","4198.93","0.28%"]],"code":"cn_600388"}]
# 'non-existence  [{"status":2,"msg":"cn_600002 non-existent","code":"cn_600002"}]
# 'non-trade      {}
    __conn=None
    __cursor=None
    
    def __init__(self,connection):
        self.__conn=connection
        self.__cursor=connection.cursor()    

    def updateHqByHs(self):
        sq="SELECT stock_code FROM listsh"
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listSh=[result[i][0] for i in range(len(result))]
        for j in range(len(listSh)):
            self.updateCodeHq(listSh[j],"20170601","20180108")
            
    def updateHqByCode(self,stockCode,startDate,endDate):
        hq=self.getHq(stockCode, startDate, endDate)
#         print(hq)
        if isinstance(hq, list):         
#             listHq=[['' for row in range(10)] for col in range(len(hq))]
            index=''
            for i in range(len(hq))[::-1]:
                sq="INSERT INTO `" + stockCode + "`(trade_date,`open`,`close`,`change`,`percent`,low,high,volume,amount,turnover) SELECT "\
#                 print(hq[i])
                for j in range(10):
                    if j==0:
                        index="'"+str(hq[i][j]).replace("-","")+"'"
                    elif j==4 or j==9:   
                        index="'"+str(hq[i][j]).replace("%","")+"'"
                    else:
                        index="'"+hq[i][j]+"'"
#                     listHq[i][j]=index
                    if j!=9:
                        sq=sq+str(index)+","
                    else:
#                    check if record exists 
                        sq=sq+index+" FROM dual WHERE NOT EXISTS(SELECT trade_date FROM `"+str(stockCode)\
                            +"` WHERE trade_date='"+str(hq[i][0]).replace("-","")+"')"
#                 print(sq)
                try:
                    self.__cursor.execute(sq)
                    print("updated:"+str(stockCode))
                except:
                    print("update error"+str(stockCode))
                
    def updateTableDate(self):
        sq="SELECT stock_code FROM tablesz LIMIT 12"
        self.__cursor.execute(sq)
        result=self.__cursor.fetchall()
        listTest=[result[i][0] for i in range(len(result))]
###################################################################################                               
    def getListHq(self,hq):
        if isinstance(hq, list):         
            listHq=[['' for row in range(10)] for col in range(len(hq))]
            index=''
            for i in range(len(hq)):
#                 print(hq[i])
                for j in range(10):
                    if j==0:
                        index=str(hq[i][j]).replace("-","")
                    elif j==4 or j==9:   
                        index=str(hq[i][j]).replace("%","")
                    else:
                        index=hq[i][j]
                    listHq[i][j]=index
#                     print(listHq[i][j])
            return listHq    
    
    def getHq(self,stockCode,startDate,endDate):
        url="http://q.stock.sohu.com/hisHq?code=cn_"+str(stockCode)\
            +"&start=" +str(startDate) + "&end=" +str(endDate)
        jsonHq=self.parseUrl(url)
#         print("return:"+str(jsonHq))
        if isinstance(jsonHq,list):
            dictHq=jsonHq[0]
            if 'hq' in dictHq:
                hq=dictHq['hq']
            else:
                hq=[] 
        else:
            hq=[]
#         print("hq:"+str(hq)) 
        return hq
      
    def parseUrl(self,strUrl):  
        sHtml=urllib.request.urlopen(strUrl)
        lReturn=json.loads(sHtml.read().decode("utf-8"))
        return lReturn
      
