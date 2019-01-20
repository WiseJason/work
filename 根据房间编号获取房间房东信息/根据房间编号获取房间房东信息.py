# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 11:29:02 2018

@author: Administrator
"""

import pandas as pd
import pymysql
import datetime
import smtplib  
from email.mime.text import MIMEText  
from email.header import Header 
dbconn=pymysql.connect(
host="rr-bp1refgx3467t7y54o.mysql.rds.aliyuncs.com",
database="gaodu",
user="gdroot",
password="gdroot",
port=3306,
charset='utf8'
)
data2=pd.read_excel("房间编号.xlsx")
data2["房间编号"]="'"+data2["房间编号"]+"'"+","
a=[]
for data in data2["房间编号"]:
    a.append(data)
a="".join(a)
a=a[:-1]

sql="SELECT a.`room_no` ,b.`business_type`  FROM gaodu.`houseroom`  as a LEFT JOIN  gaodu.`houseresource` as b on a.`resource_id` =b.`id` where a.`room_no` in  ("+a+") "
sqlcmd=sql

data3=pd.read_sql(sqlcmd,dbconn)
data3.to_excel("房间房东信息.xlsx",encoding = "gbk",index=None)

