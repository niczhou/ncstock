#coding=utf-8
import json
import urllib2

class HqUpdater:
  def getHq(stockcode,startDate,endDate):
      url="http://q.stock.sohu.com/hisHq?code=cn_" \
           +stockcode+"&start=" + startDate + "&end=" + endDate
      html=urllib2.urlopen(r+url)
      sJson=json.loads(html.read())

      print(sJson[hq])
        
    
