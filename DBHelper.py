#coding = utf-8

import pymysql

class DBHelper():
    conn = None
    cursor = None
    
    def connect_db(self,username,password):
#         print("username:%s\npassword:%s"%(username,password))
        try:
            self.conn = pymysql.connect(host="localhost",user=username,passwd=password,db="nxstock",charset="utf8")
            return self.conn
        except:
            return None
            
