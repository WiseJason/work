# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 16:05:16 2018

@author: Administrator
"""

import pandas as pd
import pymysql
import datetime


dbconn=pymysql.connect(
host="rr-bp1refgx3467t7y54o.mysql.rds.aliyuncs.com",
database="gaodu",
user="gdroot",
password="gdroot",
port=3306,
charset='utf8'
)
a="2018-07-17 00:00:00"
b="2018-07-18 00:00:00"
sql1="SELECT `mobile` AS 手机号,       FROM_UNIXTIME(`register_time`) as 注册时间,       case `renter_status` when 1 then '未租到' when 2 then '已租到' when 3 then '拒绝回访' end as 租住状态,       `renter_source` as 租房渠道,       case `is_service` when 1 then '是' when 2 then '否' end as 是否需要继续服务,       `update_man` as 回访人,       FROM_UNIXTIME(`update_time`) as 回访时间,       `bakinfo` as 备注,       case `is_look` when 1 then '是' when 2 then '否' end as 是否看房,       `is_satisfied` as NPS,       case `is_recommend` when 1 then '是' when 2 then '否' end as 是否推荐,       case `is_getcommission` when 1 then '是' when 2 then '否' end as 是否收到佣金,       FROM_UNIXTIME(`contact_lasttime`) as 最近联系时间,       `contact_count` as 联系次数,       `appoint_looktime` as 最近看房时间,       `appoint_lasttime` as 最近预约时间,       `appoint_count` as 预约次数,       `appoint_handleman` as 预约处理人,       case `is_monthly`when 1 then '是' when 2 then '否' end as 是否联系包月房源,       case `is_getcommission` when 1 then '是' when 2 then '否' end as 是否联系佣金房源,       `report_count` as 举报次数,       (CASE `city_code` WHEN 001009001 THEN '上海' WHEN 001001 THEN '北京' WHEN 001010001 THEN '南京' WHEN 001010013 THEN '苏州' WHEN 001011001 THEN '杭州' WHEN 001016001 THEN '郑州' WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州' WHEN 001019002 THEN '深圳' WHEN 001002001 THEN '天津' END) AS '城市',       `applyback_time` as 申请返现时间,       case `applyback_status` when 1 then '是' when 0 then '否' end as 申请返现,       case `is_cashback` when 1 then '是' when 2 then '否' end as 是否返现,       case `second_visit` when 1 then '需要' when 2 then '不需要' end as 二次回访,       case `visit_source` when 0 then '空' when 1 then '电话回访' when 2 then '房东反馈' when 3 then '保障房源' when 4 then '短信回访' when 5 then '返现申请' end as 回访来源,       `renter_room` as 房间编号,       `im_used` as IM次数,       FROM_UNIXTIME(`im_lasttime`) as 最近IM时间  FROM `gaodu`.`customertracking` where `update_time` BETWEEN unix_timestamp('"+a+"')   AND unix_timestamp('"+b+"')"
sqlcmd1=sql1
data1=pd.read_sql(sqlcmd1,dbconn)
data1.to_excel("租客跟踪.xlsx",encoding = "gbk",index=None)
sql2="SELECT a.`id` as 订单编号,       FROM_UNIXTIME(a.`create_time`) as 订单创建时间,       FROM_UNIXTIME(a.`update_time`) as 订单更新时间,       case a.`order_type` when 1 then '非付成交订单' when 2 then '包月成交订单' when 0 then '出房佣金(佣金成交订单) ' end as 订单类型,       case a.`order_status` when 0 then '待确认' when 1 then '确认中' when 2 then '已确认' when 3 then '无效单' when 4 then '已作废' when 5 then '已预定' end as 订单状态,       case a.`order_source` when 1 then '电话回访' when 2 then '房东反馈' when 3 then '保障房源' when 4 then '短信回访' when 5 then '返现申请' when 6 then '电话录音' end as 订单来源,       a.`check_man` as 审核人员,       a.`renter_mobile` as 租客手机号,       a.`estate_name` as 小区名称,       a.`owner_name` as 房东姓名,       a.`owner_mobile` as 房东手机号,       a.`principal_man` as 房东负责人,       (CASE a.`city_code` WHEN 001009001 THEN '上海' WHEN 001001 THEN '北京' WHEN 001010001 THEN '南京' WHEN 001010013 THEN '苏州' WHEN 001011001 THEN '杭州' WHEN 001016001 THEN '郑州' WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州' WHEN 001019002 THEN '深圳' WHEN 001002001 THEN '天津' END) AS '城市',       a.`room_no` as 房间编号,       a.`room_money` as 租金,       c.`company_name` as 品牌  FROM `gaodu`.`ownerdealorder` as a  LEFT JOIN `gaodudata`.`customer` as b on a.`owner_mobile`= b.`mobile`  LEFT JOIN `gaodustore`.`storecompanymembers` as c on b.`id`= c.`customer_id` where a.`create_time`  BETWEEN unix_timestamp('"+a+"')   AND unix_timestamp('"+b+"')"
sqlcmd2=sql2
data2=pd.read_sql(sqlcmd2,dbconn)
data2.to_excel("成交订单列表.xlsx",encoding = "gbk",index=None)
sql3="SELECT `estate_name` as 小区名称,       `feedbacker_mobile` as 反馈人手机号,       `customer_mobile` as 租客手机号,       `renter_price` as 成交金额,       `renter_monther` as 入住时长,       `room_no` as 房间编号,        `create_time`  as 反馈时间,case  `is_deal` when 0 then '未操作' when 1 then '是' when 2 then '否' end as 是否成交  FROM `gaodu`.`housedeal`where `create_time` BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"')"
sqlcmd3=sql3
data3=pd.read_sql(sqlcmd3,dbconn)
data3.to_excel("成交列表.xlsx",encoding = "gbk",index=None)