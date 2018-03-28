#coding = utf-8

import tkinter as tk
import tkinter.messagebox 
from DBHelper import DBHelper
from HqUpdater import HqUpdater

class NxUI():
    var_console = None
    et_user = None
    et_psw = None
    et_host = None
    et_db = None
    __conn = None
    
    def __init__(self):
        self.create_view()
        
    def create_view(self):
        global var_console,et_user,et_psw
        top = tk.Tk()
        top.title("nxstock")
        top.geometry("620x340")
        
        fr_login = tk.Frame(top,width=260,height=28)
        fr_login.pack(side=tk.TOP,fill=tk.X)
        
        var_psw = tk.StringVar()
        var_console = tk.StringVar()
        
        bt_login = tk.Button(fr_login,text="login",font="Arial 8",width=8,height=1)
        bt_login.pack(side=tk.RIGHT,padx=8)
        et_psw = tk.Entry(fr_login,width=8,textvariable=var_psw)
        et_psw.pack(side=tk.RIGHT,padx=6)
        et_psw['show'] = '*'
        lb_psw = tk.Label(fr_login,text="password:",font="Arial 8",width=8)
        lb_psw.pack(side=tk.RIGHT,padx=0)
        et_user = tk.Entry(fr_login,width=8)
        et_user.pack(side=tk.RIGHT,padx=6)
        lb_user = tk.Label(fr_login,text="username:",font="Arial 8",width=8)
        lb_user.pack(side=tk.RIGHT,padx=0)
        et_db = tk.Entry(fr_login,width=12)
        et_db.pack(side=tk.RIGHT,padx=6)
        lb_db = tk.Label(fr_login,text="db:",font="Arial 8",width=8)
        lb_db.pack(side=tk.RIGHT,padx=0)
        et_host = tk.Entry(fr_login,width=12)
        et_host.pack(side=tk.RIGHT,padx=6)
        lb_host = tk.Label(fr_login,text="host:",font="Arial 8",width=8)
        lb_host.pack(side=tk.RIGHT,padx=0)
        
        fr_ctl = tk.Frame(top,bg="#B0C4DE",height=168)
        fr_ctl.pack(side=tk.TOP,fill=tk.X,ipadx=1,ipady=1)
        bt_update = tk.Button(fr_ctl,text="upate hq",font="Arial 8",width=12,height=2)
        bt_update.grid(row=0,column=0,padx=8)
        bt_upzs = tk.Button(fr_ctl,text="update zs",font="Arial 8",width=12,height=2)
        bt_upzs.grid(row=0,column=1,padx=8)
        bt_uplist = tk.Button(fr_ctl,text="upate list",font="Arial 8",width=12,height=2)
        bt_uplist.grid(row=0,column=2,padx=8)
        bt_analyze = tk.Button(fr_ctl,text="analyze hq",font="Arial 8",width=12,height=2)
        bt_analyze.grid(row=0,column=3,padx=8)
        
        fr_console = tk.Frame(top)
        fr_console.pack(side=tk.BOTTOM,fill=tk.X,ipadx=1,ipady=1)
        lb_console = tk.Label(fr_console,textvariable=var_console)
        lb_console.pack(side=tk.LEFT)
        
#       bg="#fe5228"  bg="#B0C4DE"       
        bt_login['command']=self.login
        bt_update['command']=self.update_hq
        bt_upzs['command']=self.update_zs
        self.log("hi,this is nx stock mall")
        et_host.lift()
        et_db.lift()
        et_user.lift()
        et_psw.lift()
        bt_login.lift()
        top.mainloop()    
    
    def login(self):
        global var_console,et_user,et_psw,et_host,et_db
        helper = DBHelper()
        self.log(et_host.get())
#         if helper.connect_db(et_host.get(),et_user.get(),et_psw.get(),et_db.get()):
#             self.__conn = helper.connect_db(et_host.get(),et_user.get(),et_psw.get(),et_db.get())
#             self.log("login success,user: %s"%et_user.get())
#         else:
#             self.log("login fail,user: %s"%et_user.get())
    def update_hq(self):
        hqUpdater = HqUpdater(self.__conn)
        hqUpdater.updateHq()
    def update_zs(self):
        hqUpdate = HqUpdater(self.__conn)
        hqUpdate.updateZs()            
    def log(self,str_log):
        global var_console
        var_console.set(str_log)