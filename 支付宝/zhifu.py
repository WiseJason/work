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

dbconn=pymysql.connect(
host="rr-bp1refgx3467t7y54o.mysql.rds.aliyuncs.com",
database="gaodu",
user="gdroot",
password="gdroot",
port=3306,
charset='utf8'
)
sql1="SELECT b.`city_code`as '城市',c.`agent_company_name` as '品牌',c.`true_name` as'商家姓名' ,c.`mobile` as '商家电话'    FROM gaodu.`openapishow` as a LEFT JOIN gaodu.houseroom as b on a.`room_no`=b.`room_no` LEFT JOIN `gaodudata`.`customer` as c on b.`customer_id` =c.`id`   WHERE a.`is_push` =1 and a.`is_delete` =0 and a.`room_state` =2 and a.`third_type` =1 GROUP BY b.`city_code` ,c.`agent_company_name` ,c.`true_name` ,c.`mobile` "
today1 = datetime.datetime.today()+datetime.timedelta(days=-1)
a=str(datetime.datetime(today1.year, today1.month, today1.day, 0, 0, 0))
print(a)
today2= datetime.datetime.today()
b=str(datetime.datetime(today2.year, today2.month, today2.day, 0, 0, 0))
print(b)
c="2018-05-27 00:00:00"
sql2="SELECT a.mobile,a.room_id,a.owner_mobile FROM `gaodu`.`houserentercall` AS A INNER JOIN gaodu.`houseroom` AS B on B.`room_no`= A.`room_id` INNER JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id` INNER JOIN `gaodudata`.`customer` as d on a.`customer_id`= d.id where call_time BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"') and a.big_code= '4008170019' GROUP BY a.mobile,a.room_id,a.owner_mobile"
sql3="SELECT a.mobile,a.room_id,a.owner_mobile FROM `gaodu`.`houserentercall` AS A INNER JOIN gaodu.`houseroom` AS B on B.`room_no`= A.`room_id` INNER JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id` INNER JOIN `gaodudata`.`customer` as d on a.`customer_id`= d.id where call_time BETWEEN unix_timestamp('"+c+"') AND unix_timestamp('"+b+"') and a.big_code= '4008170019' GROUP BY a.mobile,a.room_id,a.owner_mobile"
sql4="SELECT a.customer_mobile as `mobile`,b.room_no as `room_id`, D.`mobile` as `owner_mobile` FROM `gaodu`.`housereservecall` as A INNER JOIN gaodu.`houseroom` AS B on B.id= A.`room_id` INNER JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id` INNER JOIN `gaodudata`.`customer` AS D on d.`id` = a.`owner_id`   WHERE A.`create_time` BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"')  and a.`gaodu_platform` =55 and a.`status` =2"
sql5="SELECT a.customer_mobile as `mobile`,b.room_no as `room_id`, D.`mobile` as `owner_mobile` FROM `gaodu`.`housereservecall` as A INNER JOIN gaodu.`houseroom` AS B on B.id= A.`room_id` INNER JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id` INNER JOIN `gaodudata`.`customer` AS D on d.`id` = a.`owner_id`    WHERE A.`create_time` BETWEEN unix_timestamp('"+c+"') AND unix_timestamp('"+b+"')  and a.`gaodu_platform` =55 and a.`status` =2"
sqlcmd1=sql1
data1=pd.read_sql(sqlcmd1,dbconn)
data1["城市"]=data1["城市"].replace("001009001","上海")
data1["城市"]=data1["城市"].replace("001001","北京")
data1["城市"]=data1["城市"].replace("001011001","杭州")
data1["城市"]=data1["城市"].replace("001010001","南京")
data1["城市"]=data1["城市"].replace("001019002","深圳")
data1["城市"]=data1["城市"].replace("001019001","广州")
data1["城市"]=data1["城市"].replace("001016001","郑州")
data1["城市"]=data1["城市"].replace("001010013","苏州")
data1["城市"]=data1["城市"].replace("001002001","天津")
data1["城市"]=data1["城市"].replace("001017001","武汉")
sqlcmd2=sql2
data2=pd.read_sql(sqlcmd2,dbconn)
print(data2)
sqlcmd4=sql4azZZ
data4=pd.read_sql(sqlcmd4,dbconn)
print(data4)
data6=pd.concat([data2,data4], axis=0)
data6.drop_duplicates(["mobile","room_id","owner_mobile"],'first',inplace = True)
print(data6)
del data6['mobile']
del data6['room_id']
data6.rename(columns={"owner_mobile":"商家电话" },inplace = True)
data6.insert(1,"当日","")
data6=data6.groupby(["商家电话"]).count().reset_index()
data6=pd.DataFrame(data6)
sqlcmd3=sql3
data3=pd.read_sql(sqlcmd3,dbconn)
sqlcmd5=sql5
data5=pd.read_sql(sqlcmd5,dbconn)
data7=pd.concat([data3,data5], axis=0)
data7.drop_duplicates(["mobile","room_id","owner_mobile"],'first',inplace = True)
del data7['mobile']
del data7['room_id']
data7.rename(columns={"owner_mobile":"商家电话" },inplace = True)
data7.insert(1,"累计","")
data7=data7.groupby(["商家电话"]).count().reset_index()
data7=pd.DataFrame(data7)
data8=pd.merge(data1,data6, how="left")
data8.fillna(0,inplace=True)
data8["当日"]=data8["当日"].astype(int)

data9=pd.merge(data8,data7, how="left")
data9.fillna(0,inplace=True)
data9["累计"]=data9["累计"].astype(int)
today=datetime.date.today() 
oneday=datetime.timedelta(days=1) 
yesterday=str(today-oneday)
filename=yesterday+".xlsx"

data9.to_excel(filename,encoding = "gbk",index=None)







