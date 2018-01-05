#coding=utf-8
class HqUtil:
      
    def getStartDate(self,inputDate,days,conn):
        sq="SELECT MIN(trade_date) FROM(SELECT trade_date FROM listdate WHERE trade_date<" \
             +str(inputDate)+" ORDER BY trade_date DESC LIMIT "+str(days)+") AS startdate"
        ##    print(sq)
        cursor=conn.cursor()
        cursor.execute(sq)
        result=cursor.fetchone()
        return result[0]
      
    def indexOfList(self,element,mArray):
        mList=None
        if type(mArray)=='tuple':
            mList=list(mArray)
        elif type(mArray)=='list':
            mList=mArray
                        
        if element in mList:
            mIndex=mList.index(element)
        else:
             mIndex=-1
             
        return mIndex
    