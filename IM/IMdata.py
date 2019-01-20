# -*- coding: utf-8 -*-
"""
Created on Wed Jul 25 10:12:31 2018

@author: Administrator
"""
import pandas as pd
import pymysql
import datetime
import smtplib  
from email.mime.text import MIMEText  
from email.header import Header 
import dateutil.parser
import smtplib
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email import encoders
import time
import calendar
from dateutil import parser

dbconn=pymysql.connect(
host="rr-bp1refgx3467t7y54o.mysql.rds.aliyuncs.com",
database="gaodu",
user="gdroot",
password="gdroot",
port=3306,
charset='utf8'
)
def getdata():
    sql="SELECT (CASE `city_code` WHEN 001009001 THEN '上海'WHEN 001001 THEN '北京'WHEN 001010001 THEN '南京'WHEN 001010013 THEN '苏州'WHEN 001011001 THEN '杭州'WHEN 001016001 THEN '郑州'WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州'WHEN 001019002 THEN '深圳'WHEN 001002001 THEN '天津' END ) AS '城市',  count(`city_code`)  as 可租房源 FROM `gaodu`.`houseroom` where `status` =2 and record_status=1 GROUP BY `city_code`  "
    sqlcmd=sql
    data=pd.read_sql(sqlcmd,dbconn)