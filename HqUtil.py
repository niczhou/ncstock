#coding=utf-8
class HqUtil:
      
    def getStartDate(self,inputDate,days,conn):
        sql="SELECT MIN(trade_date) FROM(SELECT trade_date FROM listdate WHERE trade_date<" \
             +str(inputDate)+" ORDER BY trade_date DESC LIMIT "+str(days)+") AS startdate"
        ##    print(sql)
        cursor=conn.cursor()
        cursor.execute(sql)
        result=cursor.fetchone()
        return result[0]
      

      