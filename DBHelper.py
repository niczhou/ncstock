#coding = utf-8

import pymysql

class DBHelper():
    conn = None
    cursor = None
    
    def connect_db(self,hosturl,username,password,dbname):
#         print("username:%s\npassword:%s"%(username,password))
        try:
            self.conn = pymysql.connect(host=hosturl,user=username,passwd=password,db=dbname,charset="utf8")
            return self.conn
        except:
            return None
            
