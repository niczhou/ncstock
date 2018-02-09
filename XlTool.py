import xlrd
import xlwt
import pymysql


class XlTool:
    def createXl(self,xlName,xlPath='out'):
        wBook=xlwt.Workbook(encoding = 'utf-8')
        wSheet=wBook.add_sheet('Sheet1')
        wBook.save("%s/%s.xls"%(xlPath,xlName))
    
    def tableToXl(self,connection,tableName,xlName,xlPath='out'):
        if isinstance(connection,pymysql.connections.Connection)==False:
            return 
        cursor=connection.cursor()
        
        sq="SELECT * FROM %s"%tableName
        count = cursor.execute(sq)
#         print("%d")%count
         # 重置游标的位置
        cursor.scroll(0,mode='absolute')
         # 搜取所有结果
        results = cursor.fetchall()
         
         # 获取MYSQL里面的数据字段名称
        fields = cursor.description
        workbook = xlwt.Workbook(encoding = 'utf-8')
        sheet = workbook.add_sheet(tableName,cell_overwrite_ok=True)
         
         # 写上字段信息
        for field in range(0,len(fields)):
            sheet.write(0,field,fields[field][0])
         
         # 获取并写入数据段信息
        row = 1
        col = 0
        for row in range(1,len(results)+1):
            for col in range(0,len(fields)):
                sheet.write(row,col,u'%s'%results[row-1][col])                
        workbook.save("%s/%s.xls"%(xlPath,xlName))
        print("write to excel success")