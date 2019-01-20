
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  2 10:31:04 2018

@author: bxz82
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

# =============================================================================
# def getLastDayOfLastMonth():
#     d = datetime.now()
#     c = calendar.Calendar()
#     year = d.year
#     month = d.month
#     if month == 1:
#         month = 12
#         year -= 1
#     else:
#         month -= 1
#     days = calendar.monthrange(year, month)[1]
#     return datetime(year, month, days).strftime('%Y-%m-%d')
# =============================================================================


def getFirstDayOfLastMonth():
    d = datetime.datetime.now()
    c = calendar.Calendar()
    year = d.year
    month = d.month
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    return datetime.datetime(year, month, 1).strftime('%Y-%m-%d')
# =============================================================================
# a=getFirstDayOfLastMonth()
# a=str(parser.parse(a))
# b=datetime.date.today().replace(day=1)
# b=str(datetime.datetime(b.year, b.month, b.day, 0, 0, 0))
# =============================================================================
a="2018-01-01 00:00:00"
b="2018-02-01 00:00:00"
print(a)
print(b)
sql1="SELECT A.`call_time`,   A.`gaodu_platform`,  A.`city_id`,  A.`mobile`,  A.`room_id`, a.`owner_mobile` ,    a.`big_code` FROM `gaodu`.`houserentercall` AS A   left JOIN gaodu.`houseroom` AS B on B.`room_no`= A.`room_id`   left JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id`  left JOIN `gaodudata`.`customer` AS D on a.`owner_id` = d.`id` LEFT JOIN `gaodustore`.`storecompanymembers` as e on e.`customer_id` =a.`owner_id`  where `call_time` BETWEEN unix_timestamp('"+a+"')    AND unix_timestamp('"+b+"')    and big_code<> '4008108756'    and is_marketing= 0    and status_code>= 0    and status_code<> 11    and length(`room_id`)> 1    and big_code<> '4008108782'    and is_my= 0    and A.gaodu_platform not in (14, 15, 54)"
sql2="SELECT a.`create_time` ,A.`gaodu_platform`,   A.`customer_mobile`,    A.`room_no`,  D.`mobile` as `owner_mobile` ,  A.`city_code`frOM `gaodu`.`housereservecall` as A  left JOIN gaodu.`houseroom` AS B on B.id= A.`room_id`  left JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id`  left JOIN `gaodudata`.`customer` AS D on d.`id` = a.`owner_id`  LEFT JOIN `gaodustore`.`storecompanymembers` as e on e.`customer_id` =a.`owner_id`  WHERE A.`create_time` BETWEEN unix_timestamp('"+a+"')   AND unix_timestamp('"+b+"')   and `is_marketing`= 0   and A.`status` IN(2, 5)   and is_my= 0"
sql3="SELECT `create_time`,`customer_mobile` as renter_mobile,`gaodu_platform` , `room_id` as id ,`owner_mobile` ,`city_code`   FROM `gaodu`.`yunxinhouseclick` where `create_time` BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"') GROUP BY `room_id` ,`customer_mobile` ,`owner_mobile` "
sql4="SELECT  `id` ,`room_no`  FROM `gaodu`.`houseroom` "
sqlcmd1=sql1
data1=pd.read_sql(sqlcmd1,dbconn)

sqlcmd2=sql2
data2=pd.read_sql(sqlcmd2,dbconn)
sqlcmd3=sql3
data3=pd.read_sql(sqlcmd3,dbconn)
sqlcmd4=sql4
data4=pd.read_sql(sqlcmd4,dbconn)
data1["gaodu_platform_2"]=""
def replace(x,y):
    if x=="4008150013" and y!=34:
        return 7
    if x=="4008150019" and y!=34:
        return 16
    if x=="4008151000" and y!=34:
        return 2
    if x=="4008180555" and y!=34:
        return 56
    if x=="4008196003" and y!=34:
        return 31
    if x=="4008196005" and y!=34:
        return 21
    if x=="4008170019" and y!=34:
        return 53
    if x=="4008196002" and y!=34:
        return 16
    else:
        return y
data1["gaodu_platform_2"]=pd.Series(list(map(lambda x, y:replace(x,y),data1["big_code"],data1["gaodu_platform"])))
del data1["gaodu_platform"]
data1.rename(columns={"gaodu_platform_2":"gaodu_platform"},inplace = True)
data1["call_time"]=pd.to_datetime(data1["call_time"],unit='s')
data1["渠道"]="联系"
data1["city_id"]=data1["city_id"].replace("001009001","上海")
data1["city_id"]=data1["city_id"].replace("001001","北京")
data1["city_id"]=data1["city_id"].replace("001011001","杭州")
data1["city_id"]=data1["city_id"].replace("001010001","南京")
data1["city_id"]=data1["city_id"].replace("001019002","深圳")
data1["city_id"]=data1["city_id"].replace("001019001","广州")
data1["city_id"]=data1["city_id"].replace("001016001","郑州")
data1["city_id"]=data1["city_id"].replace("001010013","苏州")
data1["city_id"]=data1["city_id"].replace("001002001","天津")
data1["city_id"]=data1["city_id"].replace("001017001","武汉")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(34,"h5")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(0,"APP")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(1,"android")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(2,"ios")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(4,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(6,"h5")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(7,"APP")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(8,"微信小程序")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(9,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(10,"APP")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(14,"APP")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(11,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(20,"APP")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(33,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(50,"APP")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(51,"支付宝小程序")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(53,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(16,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(17,"APP")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(30,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(31,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(4,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(55,"支付宝租房")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(56,"其他第三方")
data1["gaodu_platform"]=data1["gaodu_platform"].replace(21,"春眠")
data1.rename(columns={"call_time":"联系时间",
                     "city_id":"城市",
                     "mobile":"租客手机号",
                     "room_id":"房间编号",
                     "owner_mobile":"房东手机号",
                     "gaodu_platform":"租客来源",
                                     },inplace = True)
data1=data1[["房东手机号","房间编号","城市","租客手机号","租客来源"]]
data1["房东手机号"]=data1["房东手机号"].convert_objects(convert_numeric=True)

data2["create_time"]=pd.to_datetime(data2["create_time"],unit='s')

data2["渠道"]="预约"
data2["city_code"]=data2["city_code"].replace("001009001","上海")
data2["city_code"]=data2["city_code"].replace("001001","北京")
data2["city_code"]=data2["city_code"].replace("001011001","杭州")
data2["city_code"]=data2["city_code"].replace("001010001","南京")
data2["city_code"]=data2["city_code"].replace("001019002","深圳")
data2["city_code"]=data2["city_code"].replace("001019001","广州")
data2["city_code"]=data2["city_code"].replace("001016001","郑州")
data2["city_code"]=data2["city_code"].replace("001010013","苏州")
data2["city_code"]=data2["city_code"].replace("001002001","天津")
data2["city_code"]=data2["city_code"].replace("001017001","武汉")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(34,"h5")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(21,"春眠")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(0,"APP")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(1,"android")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(2,"ios")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(4,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(6,"h5")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(7,"APP")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(8,"微信小程序")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(9,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(10,"APP")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(14,"APP")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(11,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(20,"APP")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(33,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(50,"APP")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(51,"支付宝小程序")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(53,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(16,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(17,"APP")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(30,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(31,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(4,"其他第三方")
data2["gaodu_platform"]=data2["gaodu_platform"].replace(55,"支付宝")

data2.rename(columns={"create_time":"联系时间",
                     "city_code":"城市",
                     "customer_mobile":"租客手机号",
                     "room_no":"房间编号",
                     "owner_mobile":"房东手机号",
                     "gaodu_platform":"租客来源"
                    },inplace = True)
data2=data2[["房东手机号","房间编号","城市","租客手机号","租客来源"]]

data3["create_time"]=pd.to_datetime(data3["create_time"],unit='s')
data3["渠道"]="IM"
data3["city_code"]=data3["city_code"].replace("001009001","上海")
data3["city_code"]=data3["city_code"].replace("001001","北京")
data3["city_code"]=data3["city_code"].replace("001011001","杭州")
data3["city_code"]=data3["city_code"].replace("001010001","南京")
data3["city_code"]=data3["city_code"].replace("001019002","深圳")
data3["city_code"]=data3["city_code"].replace("001019001","广州")
data3["city_code"]=data3["city_code"].replace("001016001","郑州")
data3["city_code"]=data3["city_code"].replace("001010013","苏州")
data3["city_code"]=data3["city_code"].replace("001002001","天津")
data3["city_code"]=data3["city_code"].replace("001017001","武汉")
data3["gaodu_platform"]=data3["gaodu_platform"].replace(1,"android")
data3["gaodu_platform"]=data3["gaodu_platform"].replace(2,"ios")
data5=pd.merge(data3,data4,how="left")
del data5["id"]
data5.rename(columns={"create_time":"联系时间",
                     "city_code":"城市",
                     "renter_mobile":"租客手机号",
                     "room_no":"房间编号",
                     "owner_mobile":"房东手机号",
                    "gaodu_platform":"租客来源" },inplace = True)

data5=data5[["房东手机号","房间编号","城市","租客手机号","租客来源"]]
data=pd.concat([data1,data2,data5], axis=0)
data.drop_duplicates(["房东手机号","房间编号","租客手机号"],keep='first',inplace = True)
# =============================================================================
# z=list(data4["renter_phone"])
# def ResourceClassification(x,y):
#     global z
#     if x in z:
#         y="短链"
#     else:
#         y=y
# data["租客来源"]=pd.Series(list(map(lambda x, y:ResourceClassification(x,y),data["租客手机号"],data["租客来源"])))   
# =============================================================================
data.to_excel("1.xlsx",encoding = "gbk",index=None)
