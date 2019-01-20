# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 10:03:29 2018

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
data1=pd.read_excel("手机号.xlsx")
def StringBuilder(x):
    x=str(x)
    x="".join(["'",x,"'",","])
    return x
data1["id"]=pd.Series(list(map(lambda x:StringBuilder(x),data1["id"])))
a=[]
for data in data1["id"]:
    a.append(data)
a="".join(a)
a=a[:-1]

sql="SELECT a.`id` ,a.`mobile` as 手机号,       a.`true_name` as 姓名,       b.`principal_man` as BD,       c.`company_name` as 品牌,       (CASE a.`city_code` WHEN 001009001 THEN '上海' WHEN 001001 THEN '北京' WHEN 001010001 THEN '南京' WHEN 001010013 THEN '苏州' WHEN 001011001 THEN '杭州' WHEN 001016001 THEN '郑州' WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州' WHEN 001019002 THEN '深圳' WHEN 001002001 THEN '天津' END) AS '城市'  from `gaodudata`.`customer` as a  LEFT JOIN `gaodudata`.`customerinfo` as b on a.`id`= b.`customer_id`  LEFT JOIN `gaodustore`.`storecompanymembers` as c on a.`id`= c.`customer_id` where a.`id`  in ("+ a+ ") "
sqlcmd=sql
data2=pd.read_sql(sqlcmd,dbconn)
data3=pd.read_excel("端口数.xlsx")
data=pd.merge(data3,data2,how="left")
data2.to_excel("端口信息.xlsx",encoding = "gbk",index=None)

