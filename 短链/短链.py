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
a="2018-04-30 00:00:00"
b="2018-05-01 00:00:00"
sql="SELECT(CASE a.`city_id` WHEN 001009001 THEN '上海' WHEN 001001 THEN '北京' WHEN 001010001 THEN '南京' WHEN 001010013 THEN '苏州' WHEN 001011001 THEN '杭州' WHEN 001016001 THEN '郑州' WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州' WHEN 001019002 THEN '深圳' WHEN 001002001 THEN '天津' END) AS '城市',       case a.`renter_source` when 1 then '58' when 2 then '58品牌馆' when 3 then '搜房' when 4 then '365搜房' when 5 then 'app拨打失败' when 6 then '安居客' when 9 then '闲鱼' when 8 then '其他' when 7 then '赶集' end as 租客来源,       a.`renter_phone` as 租客电话,       a.`room_no` as 房间编号,       a.`estate_name` as 小区名称,       FROM_UNIXTIME(a.`contact_time`) as 联系时间,  a.`contact_phone` as 联系电话,       a.`region_name` as 区域,       a.`scope_name` as 板块,       a.`room_money` as 租金,       a.`client_phone` as 房东电话,       a.`update_man` as 处理人,       case a.`push_status` when 1 then '已推送' end as 推送状态,       a.`short_url` as 推送短链,      c.`is_commission`  as CPS, c.`is_monthly`  as CPT  FROM `gaodu`.`houserentercallshort` as a  LEFT JOIN gaodu.`houseroom` as b on a.`room_no`= b.`room_no`  LEFT JOIN `gaodudata`.`customer` as c on b.`customer_id`= c.`id` where a.`push_status`= 1   and a.`contact_time`  BETWEEN unix_timestamp('"+a+"') and unix_timestamp('"+b+"')"
sqlcmd=sql
print(sql)
data=pd.read_sql(sqlcmd,dbconn)
print(data.info())

data.insert(14,"是否付费","")
def Classification(x,y):
    if  y==1:
        return "包月"
    if x==1 and y==0:
        return "佣金"
    else:
        return "非付费"
data["是否付费"]=pd.Series(list(map(lambda x, y:Classification(x,y),data["CPS"],data["CPT"])))
del data["CPS"]
del data["CPT"]
data.to_excel("短链数据.xlsx",encoding = "gbk",index=None) 