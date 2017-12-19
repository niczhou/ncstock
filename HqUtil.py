#coding=utf-8
class HqUtil:
      
  def getStartDate(self,inputDate,days,conn):
    print("getStart")
##    sql="SELECT MIN(trade_date) FROM (SELECT trade_date FROM listDate LIMIT "+str(days)+ \
##         " WHERE trade_date<"+str(inputDate)+" ORDER BY trade_date DECS)"
    sql="SELECT MIN(trade_date) FROM(SELECT trade_date FROM listDate WHERE trade_date<" \
         +str(inputDate)+" ORDER BY trade_date DESC LIMIT "+str(days)+") AS startdate"
    print(sql)
    cursor=conn.cursor()
    cursor.execute(sql)
    result=cursor.fetchone()
    print(result[0])
    return result[0]
      
