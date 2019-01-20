

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
sql="SELECT a.`customer_id`,       a.`service_start`,       a.`service_end`,       a.`city_code`,       a.`memo`,       b.`true_name`,       b.`mobile`,       b.`is_owner`,       b.`channel`,       b.`is_monthly`,       b.`agent_company_name`,       b.`company_store_name`,       b.`is_port`,       c.`principal_man`,       c.`owner_remark`,       c.`region_name`  FROM gaodudata.`customerservicedate` as a  left JOIN gaodudata.`customer` as b on a.`customer_id`= b.`id`  LEFT JOIN gaodudata.`customerinfo` as c on a.`customer_id`= c.`customer_id`where a.service_end> UNIX_TIMESTAMP('2018-07-09 00:00:00')   and a.service_start< UNIX_TIMESTAMP('2018-07-16 00:00:00')"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("基本信息.xlsx",encoding = "gbk",index=None)
sql="SELECT a.`customer_id`,       count(d.`operate_type`) as '审核通过数'  from (select * from gaodudata.`customerservicedate`  where `service_end` > UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start` < UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a   left JOIN gaodudata.`customer` as b on a.`customer_id`= b.`id`  left join gaodu.houseroom as c on a.`customer_id`= c.`customer_id`  left join gaodu.`houseupdatelog` as d on c.`id`= d.`house_id` where d.update_time BETWEEN unix_timestamp('2018-07-09 00:00:00')   AND unix_timestamp('2018-07-16 00:00:00')   and d.operate_type ='审核通过'GROUP BY a.`customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("审核通过.xlsx",encoding = "gbk",index=None)

sql="SELECT a.`customer_id`,       count(d.`operate_type`) as '审核不通过数'  from (select * from gaodudata.`customerservicedate`  where `service_end` > UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start` < UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a   left JOIN gaodudata.`customer` as b on a.`customer_id`= b.`id`  left join gaodu.houseroom as c on a.`customer_id`= c.`customer_id`  left join gaodu.`houseupdatelog` as d on c.`id`= d.`house_id` where d.update_time BETWEEN unix_timestamp('2018-07-09 00:00:00')   AND unix_timestamp('2018-07-16 00:00:00')   and d.operate_type ='审核不通过'GROUP BY a.`customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("审核不通过.xlsx",encoding = "gbk",index=None)
sql="SELECT b.`customer_id`,     (select from_unixtime(max(create_time))  from gaoducollect.`loginlogowner` where customer_id= a.id) as '最近登录时间'from `gaodudata`.`customer` AS A RIGHT JOIN  gaodudata.`customerservicedate` as b on a.`id` =b.`customer_id` where b.service_end> UNIX_TIMESTAMP('2018-07-09 00:00:00')   and b.service_start< UNIX_TIMESTAMP('2018-07-16 00:00:00')GROUP BY `customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("最近登录时间.xlsx",encoding = "gbk",index=None)
sql="SELECT a.`customer_id`,       count(DISTINCT(FROM_UNIXTIME(b.`create_time`,'%Y%m%d')))  as '登陆天数' ,  count(b.`create_time`) as '登陆次数'   FROM  (select * from gaodudata.`customerservicedate`  where `service_end` > UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start` < UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a left JOIN `gaoducollect`.`loginlogowner`  as  b on a.`customer_id` =b.`customer_id`where b.`create_time` BETWEEN unix_timestamp('2018-07-09 00:00:00')   AND unix_timestamp('2018-07-16 00:00:00') GROUP BY a. `customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("登录天数次数.xlsx",encoding = "gbk",index=None)
sql="SELECT a.`customer_id`,       count(DISTINCT(FROM_UNIXTIME(b.`create_time`,'%Y%m%d')))  as '刷新天数' ,  count(DISTINCT(FROM_UNIXTIME(b.`create_time`,'%Y%m%d %H'))) as '刷新次数'   FROM  (select * from gaodudata.`customerservicedate`  where `service_end` > UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start` < UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a left JOIN gaodu.`houserefresh`   as  b on a.`customer_id` =b.`customer_id`where b.`create_time` BETWEEN unix_timestamp('2018-07-09 00:00:00')   AND unix_timestamp('2018-07-16 00:00:00') GROUP BY a. `customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("刷新天数、次数.xlsx",encoding = "gbk",index=None)
sql="SELECT a. `customer_id`,	count(b.`room_no` ) as '可租房源数'  from(select `customer_id`   from gaodudata.`customerservicedate` where `service_end`> UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start`< UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a INNER  JOIN gaodu.`houseroom` as b on a.`customer_id` =b.`customer_id` where  b.status=2 and b.record_status=1 GROUP BY a.`customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("可租房源数.xlsx",encoding = "gbk",index=None)
sql="SELECT a. `customer_id`,	count(b.`room_no` ) as '视频房源数'  from(select `customer_id`   from gaodudata.`customerservicedate` where `service_end`> UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start`< UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a INNER  JOIN gaodu.`houseroom` as b on a.`customer_id` =b.`customer_id` where b.status=2 and b.record_status=1 and b.had_vedio=1 GROUP BY a.`customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("视频房源数.xlsx",encoding = "gbk",index=None)
sql="SELECT b.`customer_id`,       c.`status_code`,b.`city_code` ,count(c.`status_code` )  from(select *  from gaodudata.`customerservicedate` where `service_end`> UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start`< UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a INNER  JOIN gaodu.`houseroom` as b on a.`customer_id` =b.`customer_id` inner join gaodu.`houserentercall` as c on b.`room_no` =c.`room_id`  where c.`create_time` BETWEEN unix_timestamp('2018-07-09 00:00:00')   AND unix_timestamp('2018-07-16 00:00:00') GROUP BY a.`customer_id` ,c.`status_code`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("电话数.xlsx",encoding = "gbk",index=None)
sql="SELECT a.`customer_id_from`,       a.`customer_id_to`,       FROM_UNIXTIME(MIN(a.`create_time`) /1000) ,       a.`city_code`,       a.to_is_owner,       a.`from_is_owner`  FROM gaodu.`yunxinmessage` a WHERE a.create_time>= UNIX_TIMESTAMP('2018-07-09 00:00:00') *1000   AND a.create_time< UNIX_TIMESTAMP('2018-07-16 00:00:00') *1000 GROUP BY a.`customer_id_from`,         a.`customer_id_to` order by `create_time`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("IM5分钟.xlsx",encoding = "gbk",index=None)
sql="SELECT a.`customer_id`,       count(b.`create_time`)  FROM(select `customer_id`  from gaodudata.`customerservicedate` where `service_end`> UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start`< UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a  left JOIN `gaodudata`.`customer` as c on a.`customer_id`= c.`id`   left JOIN `gaodu`.`housereservecall` as b on b.`owner_mobile`= c.`mobile` where b.`create_time` BETWEEN unix_timestamp('2018-07-09 00:00:00')   AND unix_timestamp('2018-07-16 00:00:00') GROUP BY a.`customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("预约总量.xlsx",encoding = "gbk",index=None)
sql="SELECT a.`customer_id`,       count(b.`create_time`)  FROM(select `customer_id`  from gaodudata.`customerservicedate` where `service_end`> UNIX_TIMESTAMP('2018-07-09 00:00:00')   and `service_start`< UNIX_TIMESTAMP('2018-07-16 00:00:00')) as a  left JOIN `gaodudata`.`customer` as c on a.`customer_id`= c.`id`   left JOIN `gaodu`.`housereservecall` as b on b.`owner_mobile`= c.`mobile` where b.`create_time` BETWEEN unix_timestamp('2018-07-09 00:00:00')   AND unix_timestamp('2018-07-16 00:00:00') and `status` =2 GROUP BY a.`customer_id`"
sqlcmd=sql
data=pd.read_sql(sqlcmd,dbconn)
data.to_excel("预约成功.xlsx",encoding = "gbk",index=None)
