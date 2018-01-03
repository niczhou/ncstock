#coding=utf-8
import json
import time
import urllib.request
import urllib.error

class HqUpdater:
      
# 'url            http://q.stock.sohu.com/hisHq?code=cn_300228&start=20170413&end=20170417
# 'return         [{"Status":0,"hq":[["2017-04-27","15.46","15.46","1.41","10.04%","14.89","15.46","1460797","222747.31","13.66%"],
# '               ["2017-04-26","14.05","14.05","1.28","10.02%","14.05","14.05","29886","4198.93","0.28%"]],"code":"cn_600388"}]
# 'non-existence  [{"status":2,"msg":"cn_600002 non-existent","code":"cn_600002"}]
# 'non-trade      {}
    def updateHq(self,stockCode,startDate,endDate):
        hq=self.getHq(stockcode, startDate, endDate)
        if isinstance(hq, list):         
#             listHq=[['' for row in range(10)] for col in range(len(hq))]
            index=''
            for i in range(len(hq))[::-1]:
                sq="INSERT INTO `" + stockCode + "`(trade_date,`open`,`close`,`change`,`percent`,low,high,volume,amount,turnover) VALUES("
                print(hq[i])
                for j in range(10):
                    if j==0:
                        index=str(hq[i][j]).replace("-","")
                    elif j==4 or j==9:   
                        index=str(hq[i][j]).replace("%","")
                    else:
                        index=hq[i][j]
#                     listHq[i][j]=index
                    if j!=9:
                        sq=sq+str(index)+","
                    else:
                        sq=sq+index+")"
                print(sq)

                               
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
                    print(listHq[i][j])
            return listHq
            
      
    
    def getHq(stockCode,startDate,endDate):
        url="http://q.stock.sohu.com/hisHq?code=cn_" \
             +stockCode+"&start=" + startDate + "&end=" + endDate
        mDict=self.parseUrl(url)
        if 'hq' in mDict:
            hq=mDict['hq']
        else:
            hq=[]
        return hq
      
    def parseUrl(self,strUrl):  
        sHtml=urllib.request.urlopen(strUrl)
        sDict=json.loads(sHtml.read().decode("utf-8"))
        return sDict
      
