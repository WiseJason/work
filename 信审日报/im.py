# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 10:08:27 2018

@author: Administrator
"""
import pandas as pd
import pymysql
import datetime
import smtplib  
from email.mime.text import MIMEText  
from email.header import Header 

import smtplib
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email import encoders
import time
import calendar
dbconn=pymysql.connect(
host="rr-bp1refgx3467t7y54o.mysql.rds.aliyuncs.com",
database="gaodu",
user="gdroot",
password="gdroot",
port=3306,
charset='utf8'
)
def classification2(x):
    if x=="上海" or x=="北京" or x=="苏州" or x=="天津":
        return "总部信审品控"
    if x=="杭州" or x=="深圳" or x=="广州":
        return "杭分运营"
    if x=="南京" or x=="郑州" or x=="武汉":
        return "宁分运营"
data2=pd.read_excel(r'C:\Users\Administrator\Desktop\信审日报\IM.xlsx')
data2["房间编号"]="'"+data2["房间编号"]+"'"+","
a=[]
for data in data2["房间编号"]:
    a.append(data)
a="".join(a)
a=a[:-1]

sql="SELECT a.`room_no` as 房间编号, b.`estate_name` as 小区名称, a.`principal_man` as 房东负责人,      d.`company_name` as 品牌,       (CASE a.`city_code` WHEN 001009001 THEN '上海' WHEN 001001 THEN '北京' WHEN 001010001 THEN '南京' WHEN 001010013 THEN '苏州' WHEN 001011001 THEN '杭州' WHEN 001016001 THEN '郑州' WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州' WHEN 001019002 THEN '深圳' WHEN 001002001 THEN '天津' END) AS '城市'  FROM `gaodu`.`houseroom` as a  LEFT JOIN gaodu.`houseresource` as b on a.`resource_id`= b.`id`  LEFT JOIN `gaodudata`.`customer` as c on a.`customer_id`= c.`id`  LEFT JOIN `gaodustore`.`storecompanymembers` as d on a.`customer_id`= d.`customer_id` where a.`room_no`  in "+"("+a+")"
sqlcmd=sql
data1=pd.read_sql(sqlcmd,dbconn)
data3=pd.read_excel(r'C:\Users\Administrator\Desktop\信审日报\IM.xlsx')
# =============================================================================
# print(data1)
# =============================================================================

data=pd.merge(data3,data1, how="left")
data.insert(2,"渠道","IM")
data.insert(12,"房源负责人","")
data.insert(12,"操作","已出租")
data.insert(12,"责任人","")
data["责任人"]=pd.Series(list(map(lambda x:classification2(x),data["城市"])))
data=data[["城市","渠道","房间编号","小区名称","品牌","身份","租客手机号","房东手机号","房东姓名","房源负责人","房东负责人","记录人","最近聊天时间","操作","问题记录","责任人"]]   

data.to_excel(r'C:\Users\Administrator\Desktop\信审日报\IM_New.xlsx',encoding = "gbk",index=None)
