import xlrd
import xlwt

class XlTool:
    def createXl(self,xlName,xlPath='out'):
        wBook=xlwt.Workbook(encoding = 'utf-8')
        wSheet=wBook.add_sheet('Sheet1')
        wBook.save("%s/%s.xls"%(xlPath,xlName))
    
    def writeSheet(self,xlSheet,*Args2D):
        if isinstance(xlSheet,xlrd.sheet):
            pass