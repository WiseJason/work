# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 17:39:01 2018

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
sql1="select e.`mobile`as 手机号 ,      d.time as端口到期时间  from `gaodudata`.`businessillegalinfo` as e  LEFT JOIN `gaodudata`.`customer` as a on a.`id`= e.`customer_id`  LEFT JOIN `gaodudata`.`customerinfo` as b on a.`id`= b.`customer_id`  LEFT JOIN `gaodustore`.`storecompanymembers` as c on a.`id`= c.`customer_id`  LEFT JOIN(SELECT `customer_id`, from_unixtime(max(`service_end`),'%y%m%d') as time  from `gaodudata`.`customerservicedate` GROUP BY `customer_id`) as d on a.`id`= d.`customer_id` where e.`status`= 2"
sqlcmd1=sql1#端口到期日期
data1=pd.read_sql(sqlcmd1,dbconn)
sql2="select e.`mobile` as 手机号, FROM_UNIXTIME(max(e.`create_time`), '%y%m%d') as 最近违规时间 from `gaodudata`.`businessillegalinfo` as e  LEFT JOIN `gaodudata`.`customer` as a on a.`id`= e.`customer_id`  LEFT JOIN `gaodudata`.`customerinfo` as b on a.`id`= b.`customer_id`  LEFT JOIN `gaodustore`.`storecompanymembers` as c on a.`id`= c.`customer_id`  LEFT JOIN(SELECT `customer_id`, from_unixtime(max(`service_end`)) as time  from `gaodudata`.`customerservicedate` GROUP BY `customer_id`) as d on a.`id`= d.`customer_id` where e.`status`= 2 GROUP BY a.`mobile`"
sqlcmd2=sql2
data2=pd.read_sql(sqlcmd2,dbconn)#最近违规时间
sql3="SELECT a.`mobile` as 手机号,       b.`true_name` as 房东姓名,       (CASE b.`city_code` WHEN 001009001 THEN '上海' WHEN 001001 THEN '北京' WHEN 001010001 THEN '南京' WHEN 001010013 THEN '苏州' WHEN 001011001 THEN '杭州' WHEN 001016001 THEN '郑州' WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州' WHEN 001019002 THEN '深圳' WHEN 001002001 THEN '天津' END) AS '城市',       c.`company_name` as 品牌,       d.`principal_man` as BD,       sum(a.`times`) as 累计违规次数  FROM `gaodudata`.`businessillegalinfo` as a  LEFT JOIN `gaodudata`.`customer` as b on a.`customer_id`= b.`id`  LEFT JOIN `gaodustore`.`storecompanymembers` as c on a.`customer_id`= c.`customer_id`  LEFT JOIN `gaodudata`.`customerinfo` as d on a.`customer_id`= d.`customer_id` GROUP BY a.`mobile`"
sqlcmd3=sql3
data3=pd.read_sql(sqlcmd3,dbconn)
data=pd.merge(data3,data1, how="left")
data=pd.merge(data,data2, how="left")
data.to_excel("严重违规.xlsx",encoding = "gbk",index=None)