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
    def getHqlist(self,hq):
        hqList=[]
        index=''
        if isinstance(hq, list):         
            for i in range(0,len(hq)):
#                 print(hq[i])
                for j in range(0,len(hq[i])):
                    if j==0:
                        index=str(hq[i][j]).replace("-","")
                    elif j==4 or j==9:   
                        index=str(hq[i][j]).replace("%","")
                    else:
                        index=hq[i][j]
                    hqList[i][j]=index
                    print(hqList[i][j])
            
      
    
    def getHq(stockcode,startDate,endDate):
        url="http://q.stock.sohu.com/hisHq?code=cn_" \
             +stockcode+"&start=" + startDate + "&end=" + endDate
        mDict=self.parseUrl(url)
        hq=mDict['hq']
        return hq
      
    def parseUrl(self,strUrl):  
        sHtml=urllib.request.urlopen(strUrl)
        sDict=json.loads(sHtml.read().decode("utf-8"))
        return sDict
      
