# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 09:20:27 2018

@author: Administrator
"""


import smtplib
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email import encoders
import time
import calendar
import pymysql
import pandas as pd
dbconn=pymysql.connect(
host="rr-bp1refgx3467t7y54o.mysql.rds.aliyuncs.com",
database="gaodu",
user="gdroot",
password="gdroot",
port=3306,
charset='utf8'
)
sql1="SELECT b.`room_no` as 房间编号,       FROM_UNIXTIME(a.`update_time`) as 品控下架时间  FROM gaodu.`houseroom` as b  LEFT JOIN `gaodu`.`houseupdatelog` as a on b.`id`= a.`house_id` where  a.`operate_type` in('已出租', '删除房源', '删除房间')   and a.`update_time` BETWEEN  unix_timestamp('2018-04-01 00:00:00') AND unix_timestamp('2018-07-18 00:00:00') GROUP BY b.`room_no` "
sqlcmd1=sql1
data1=pd.read_sql(sqlcmd1,dbconn)
sql12="SELECT b.`room_no` as 房间编号, FROM_UNIXTIME(a.`create_time`) as 上架时间  FROM gaodu.`houseroom` as b  LEFT JOIN `gaodu`.`houseupdatelog` as a on b.`id`= a.`house_id` where  a.`operate_type` like '%上架操作%'   and  a.`update_time` BETWEEN unix_timestamp('2018-04-01 00:00:00') AND unix_timestamp('2018-07-18 00:00:00')"
sqlcmd12=sql12
data2=pd.read_sql(sqlcmd12,dbconn)
data1.to_excel("最近品控下架1.xlsx",encoding = "gbk",index=None)
data2.to_excel("上架时间1.xlsx",encoding = "gbk",index=None)
data=pd.merge(data2,data1,how="left")
data["时间差"]=(data["上架时间"]-data["最近品控下架时间"]).days()


data.to_excel("上下架1.xlsx",encoding = "gbk",index=None)
