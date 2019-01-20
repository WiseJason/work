# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 14:02:01 2018

@author: bxz82
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jun 26 18:02:50 2018

@author: bxz82
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
def getdata():
    a="2018-04-01 00:00:00"
    b="2018-07-23 00:00:00"
    print(b)
    sql1="SELECT from_unixtime(a.call_time) as 时间, a.city_id as 城市,c.region_name as 区域,c.scope_name as 商圈,c.estate_name as 小区名称,c.room_type as 房间类型,c.room_num as 几室,b.room_no as 房间编号,b.room_money as 租金,b.info_resource as 来源,b.is_commission as 是否佣金,a.mobile as 租客手机号,d.agent_company_name as 中介公司,a.owner_mobile as 房东手机号,a.owner_name as 房东姓名 ,e.`company_name`  as 品牌,d.company_store_name as 门店,a.gaodu_platform as 电话来源,a.big_code,a.charge_man as 房源负责人,a.principal_man as 房东负责人,a.status_code as 电话状态,a.caller_length as 主叫时长,a.called_length as 被叫时长,b.is_monthly as 是否包月  FROM gaodu.houserentercall AS A  left JOIN gaodu.houseroom AS B on B.room_no= A.room_id  left JOIN gaodu.houseresource AS C on B.resource_id= C.id  left JOIN gaodudata.customer as d on a.`owner_id`= d.id left join gaodustore.storecompanymembers as e on  a.owner_id=e.customer_id where a.call_time BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"')"
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
    data1["房间类型"]=data1["房间类型"].replace("0201","合租原房")
    data1["房间类型"]=data1["房间类型"].replace("0202","合租N+1")
    data1["房间类型"]=data1["房间类型"].replace("0203","合租隔断房")
    data1["房间类型"]=data1["房间类型"].replace("0204","整租单间")
    data1["房间类型"]=data1["房间类型"].replace("0205","整租套间")
    data1["电话来源"]=data1["电话来源"].replace(34,"蚂蚁")
    data1["电话来源"]=data1["电话来源"].replace(21,"春眠")
    data1["电话来源"]=data1["电话来源"].replace(0,"APP")
    data1["电话来源"]=data1["电话来源"].replace(1,"APP")
    data1["电话来源"]=data1["电话来源"].replace(2,"APP")
    data1["电话来源"]=data1["电话来源"].replace(4,"活动")
    data1["电话来源"]=data1["电话来源"].replace(6,"APP")
    data1["电话来源"]=data1["电话来源"].replace(7,"APP")
    data1["电话来源"]=data1["电话来源"].replace(8,"APP")
    data1["电话来源"]=data1["电话来源"].replace(9,"百度租房")
    data1["电话来源"]=data1["电话来源"].replace(10,"APP")
    data1["电话来源"]=data1["电话来源"].replace(56,"搜狗")
    data1["电话来源"]=data1["电话来源"].replace(11,"open_api")
    data1["电话来源"]=data1["电话来源"].replace(20,"APP")
    data1["电话来源"]=data1["电话来源"].replace(33,"360")
    data1["电话来源"]=data1["电话来源"].replace(50,"APP")
    data1["电话来源"]=data1["电话来源"].replace(51,"APP")
    data1["电话来源"]=data1["电话来源"].replace(53,"分期乐")
    data1["电话来源"]=data1["电话来源"].replace(16,"百度推广")
    data1["电话来源"]=data1["电话来源"].replace(17,"APP")
    data1["电话来源"]=data1["电话来源"].replace(30,"豆瓣")
    data1["电话来源"]=data1["电话来源"].replace(31,"微博")
    data1["电话来源"]=data1["电话来源"].replace(55,"支付宝租房")
    data1["电话状态"]=data1["电话状态"].replace(0,"成功")
    data1["电话状态"]=data1["电话状态"].replace(1,"忙")
    data1["电话状态"]=data1["电话状态"].replace(2,"无应答")
    data1["电话状态"]=data1["电话状态"].replace(3,"客户提前挂机")
    data1["电话状态"]=data1["电话状态"].replace(11,"客户主动放弃")
    data1["电话状态"]=data1["电话状态"].replace(201,"无效分机号")
    data1["电话状态"]=data1["电话状态"].replace(555,"黑名单")
    data1["电话状态"]=data1["电话状态"].replace(777,"回呼外线失败")
    data1["电话状态"]=data1["电话状态"].replace(1000,"非工作时间")
    data1["电话状态"]=data1["电话状态"].replace(1002,"欠费")
    data1["电话状态"]=data1["电话状态"].replace(-1,"未知")
    def replace(x,y):
        if x==4008150013 and y!=34:
            return 7
        if x==4008150019 and y!=34:
            return 16
        if x==4008151000 and y!=34:
            return 2
        if x==4008180555 and y!=34:
            return 56
        if x==4008196003 and y!=34:
            return 31
        if x==4008196005 and y!=34:
            return 21
        if x==4008170019 and y!=34:
            return 53
        else:
            return y
    data1["电话来源"]=pd.Series(list(map(lambda x, y:replace(x,y),data1["big_code"],data1["电话来源"])))
    del data1["big_code"]
    data1.to_excel("dianhua.xlsx",encoding = "gbk",index=None)
    sql2="SELECT from_unixtime(a.create_time ) as 时间, a.city_code as 城市,  e.`company_name` as 品牌,b.principal_man as 负责人,d.true_name as 房东姓名,a.owner_mobile as 房东电话,a.renter_mobile as 租客电话, from_unixtime(a.create_time ) as 开始联系时间,c.estate_name as 小区名称,c.region_name as 区域,c.scope_name  as 商圈,b.room_no as 房间编号,b.room_money as 租金 FROM gaodu.summaryconnect AS a LEFT JOIN gaodu.houseroom AS b  on a.room_id= b.id left JOIN gaodu.houseresource AS C on B.resource_id= C.id  left JOIN gaodudata.customer AS D on d.id =a.owner_id left JOIN `gaodustore`.`storecompanymembers` as e on a.`owner_id`  =e.`customer_id`   WHERE a.type =3   and a.create_time BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"')"
    sqlcmd2=sql2
    data2=pd.read_sql(sqlcmd2,dbconn)
    data2.insert(7,"租客来源","APP")
    data2["城市"]=data2["城市"].replace("001009001","上海")
    data2["城市"]=data2["城市"].replace("001001","北京")
    data2["城市"]=data2["城市"].replace("001011001","杭州")
    data2["城市"]=data2["城市"].replace("001010001","南京")
    data2["城市"]=data2["城市"].replace("001019002","深圳")
    data2["城市"]=data2["城市"].replace("001019001","广州")
    data2["城市"]=data2["城市"].replace("001016001","郑州")
    data2["城市"]=data2["城市"].replace("001010013","苏州")
    data2["城市"]=data2["城市"].replace("001002001","天津")
    data2["城市"]=data2["城市"].replace("001017001","武汉")
    data2.to_excel("im.xlsx",encoding = "gbk",index=None)
    sql3="SELECT a.city_code as 城市,from_unixtime(a.create_time) as 提交时间,   a.customer_name as 预约人姓名, a.customer_mobile as 预约人手机, a.resource_no as 预约房源, a.room_no as 预约房间,       a.owner_mobile as 房东手机, a.owner_name as 房东姓名, from_unixtime(a.handle_time) as 处理时间,       a.handle_man as 处理人, a.status as 处理状态, from_unixtime(a.look_time) as 看房时间, a.handle_reason as 理由, a.gaodu_platform as 来源, c.info_resource as 数据来源, e.`company_name`   as 品牌, d.company_store_name as 门店, b.is_commission as 是否佣金,  b.is_monthly as 是否包月, c.region_name as 区域, c.scope_name as 商圈, b.room_money as 租金, a.room_type as 房间类型, c.estate_name as 小区名称, b.principal_man as 房东负责人, a.record_status as 预约状态  FROM gaodu.housereservecall as A  left JOIN gaodu.houseroom AS B on B.id= A.room_id  left JOIN gaodu.houseresource AS C on B.resource_id= C.id  left JOIN gaodudata.customer AS D on d.id= a.`owner_id` left JOIN `gaodustore`.`storecompanymembers` as e on a.`owner_id`  =e.`customer_id`  WHERE A.create_time BETWEEN unix_timestamp('"+a+"')   AND unix_timestamp('"+b+"')"
    sqlcmd3=sql3
    data3=pd.read_sql(sqlcmd3,dbconn)    
    data3["城市"]=data3["城市"].replace("001009001","上海")
    data3["城市"]=data3["城市"].replace("001001","北京")
    data3["城市"]=data3["城市"].replace("001011001","杭州")
    data3["城市"]=data3["城市"].replace("001010001","南京")
    data3["城市"]=data3["城市"].replace("001019002","深圳")
    data3["城市"]=data3["城市"].replace("001019001","广州")
    data3["城市"]=data3["城市"].replace("001016001","郑州")
    data3["城市"]=data3["城市"].replace("001010013","苏州")
    data3["城市"]=data3["城市"].replace("001002001","天津")
    data3["城市"]=data3["城市"].replace("001017001","武汉")
    data3["来源"]=data3["来源"].replace(34,"蚂蚁")
    data3["来源"]=data3["来源"].replace(21,"春眠")
    data3["来源"]=data3["来源"].replace(0,"APP")
    data3["来源"]=data3["来源"].replace(1,"APP")
    data3["来源"]=data3["来源"].replace(2,"APP")
    data3["来源"]=data3["来源"].replace(4,"活动")
    data3["来源"]=data3["来源"].replace(6,"APP")
    data3["来源"]=data3["来源"].replace(7,"APP")
    data3["来源"]=data3["来源"].replace(8,"APP")
    data3["来源"]=data3["来源"].replace(9,"百度租房")
    data3["来源"]=data3["来源"].replace(10,"APP")
    data3["来源"]=data3["来源"].replace(56,"搜狗")
    data3["来源"]=data3["来源"].replace(11,"open_api")
    data3["来源"]=data3["来源"].replace(20,"APP")
    data3["来源"]=data3["来源"].replace(33,"360")
    data3["来源"]=data3["来源"].replace(50,"APP")
    data3["来源"]=data3["来源"].replace(51,"APP")
    data3["来源"]=data3["来源"].replace(53,"分期乐")
    data3["来源"]=data3["来源"].replace(16,"百度推广")
    data3["来源"]=data3["来源"].replace(17,"APP")
    data3["来源"]=data3["来源"].replace(30,"豆瓣")
    data3["来源"]=data3["来源"].replace(31,"微博")
    data3["来源"]=data3["来源"].replace(55,"支付宝租房")
    data3["处理状态"]=data3["处理状态"].replace(0,"未处理")
    data3["处理状态"]=data3["处理状态"].replace(1,"处理中")
    data3["处理状态"]=data3["处理状态"].replace(2,"成功")
    data3["处理状态"]=data3["处理状态"].replace(3,"取消")
    data3["处理状态"]=data3["处理状态"].replace(4,"暂停")
    data3["处理状态"]=data3["处理状态"].replace(5,"失败")
    data3["处理状态"]=data3["处理状态"].replace(9,"已配单")    
    data3["预约状态"]=data3["预约状态"].replace(0,"删除")
    data3["预约状态"]=data3["预约状态"].replace(1,"未删除")
    data3["房间类型"]=data3["房间类型"].replace("0201","合租原房")
    data3["房间类型"]=data3["房间类型"].replace("0202","合租N+1")
    data3["房间类型"]=data3["房间类型"].replace("0203","合租隔断房")
    data3["房间类型"]=data3["房间类型"].replace("0204","整租单间")
    data3["房间类型"]=data3["房间类型"].replace("0205","整租套间")
    data3.to_excel("yuyue.xlsx",encoding = "gbk",index=None)

def get_email_obj(email_subject, email_from, to_addr_list):
    '''
    构造邮件对象，并设置邮件主题、发件人、收件人，最后返回邮件对象
    :param email_subject:邮件主题
    :param email_from:发件人
    :param to_addr_list:收件人列表
    :return :邮件对象 email_obj
    '''
    # 构造 MIMEMultipart 对象做为根容器
    email_obj = MIMEMultipart()
    email_to = ','.join(to_addr_list)   # 将收件人地址用“,”连接
    # 邮件主题、发件人、收件人
    email_obj['Subject'] = Header(email_subject, 'utf-8')
    email_obj['From'] = Header(email_from, 'utf-8')
    email_obj['To'] = Header(email_to, 'utf-8')
    return email_obj
 
 
def attach_content(email_obj, email_content, content_type='plain', charset='utf-8'):
    '''
    创建邮件正文，并将其附加到跟容器：邮件正文可以是纯文本，也可以是HTML（为HTML时，需设置content_type值为 'html'）
    :param email_obj:邮件对象
    :param email_content:邮件正文内容
    :param content_type:邮件内容格式 'plain'、'html'..，默认为纯文本格式 'plain'
    :param charset:编码格式，默认为 utf-8
    :return:
    '''
    content = MIMEText(email_content, content_type, charset)    # 创建邮件正文对象
    email_obj.attach(content)     # 将邮件正文附加到根容器
 
 
def attach_part(email_obj, source_path, part_name):
    '''
    添加附件：附件可以为照片，也可以是文档
    :param email_obj:邮件对象
    :param source_path:附件源文件路径
    :param part_name:附件名
    :return:
    '''
    part = MIMEBase('application', 'octet-stream')                          # 'octet-stream': binary data   创建附件对象
    part.set_payload(open(source_path, 'rb').read())                        # 将附件源文件加载到附件对象
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="%s"' % part_name)     # 给附件添加头文件
    email_obj.attach(part)                                                                # 将附件附加到根容器
 
 
def send_email(email_obj, email_host, host_port, from_addr, pwd, to_addr_list):
    '''
    发送邮件
    :param email_obj:邮件对象
    :param email_host:SMTP服务器主机
    :param host_port:SMTP服务端口号
    :param from_addr:发件地址
    :param pwd:发件地址的授权码，而非密码
    :param to_addr_list:收件地址
    :return:发送成功，返回 True；发送失败，返回 False
    '''
    try:
        '''
            # import smtplib
            # smtp_obj = smtplib.SMTP([host[, port[, local_hostname]]] )
                # host: SMTP服务器主机。
                # port: SMTP服务端口号，一般情况下SMTP端口号为25。
            # smtp_obj = smtplib.SMTP('smtp.qq.com', 25)
        '''
        smtp_obj = smtplib.SMTP_SSL(email_host, host_port)     # 连接 smtp 邮件服务器
        smtp_obj.login(from_addr, pwd)
        smtp_obj.sendmail(from_addr, to_addr_list, email_obj.as_string())  # 发送邮件：email_obj.as_string()：发送的信息
        smtp_obj.quit()                                 # 关闭连接
        print("发送成功！")
        return True
    except smtplib.SMTPException:
        print("发送失败！")
        return False
 
 
if __name__ == "__main__":
    # （QQ邮箱）
    email_host = "smtp.exmail.qq.com"            # smtp 邮件服务器
    host_port = 465                       # smtp 邮件服务器端口：SSL 连接
    from_addr = ""                # 发件地址
    pwd = ""                  # 发件地址的授权码，而非密码
 
    # （163邮箱）
    # email_host = "smtp.163.com"             # smtp 邮件服务器
    # host_port = 465                         # smtp 邮件服务器端口：SSL 连接
    # from_addr = "发件地址"                  # 发件地址
    # pwd = "授权码"                    # 发件地址的授权码，而非密码
 
    to_addr_list = ["tongmengjie@hizhu.com","wanghui@hizhu.com","zhangxiaojun@hizhu.com"]       # 收件地址

    email_content = "链接数据"
    email_content_html = """
    <p>Python 邮件发送...</p>
    <p><a href="http://www.runoob.com">菜鸟教程链接</a></p>
    <p>图片：</p>
    <p><img src="cid:image1"></p>
    """
    
    email_subject ="链接数据"
    email_from = "童梦杰"

    doc={"dianhua.xlsx":"dianhua.xlsx","im.xlsx":"im.xlsx","yuyue.xlsx":"yuyue.xlsx"}
    getdata()
# =============================================================================
#     for k,v in doc.items():
#         source_path=k
#         part_name=v
#         email_obj = get_email_obj(email_subject, email_from, to_addr_list)
#         attach_content(email_obj, email_content)
#         attach_part(email_obj, source_path, part_name)
#         send_email(email_obj, email_host, host_port, from_addr, pwd, to_addr_list)
# =============================================================================

  





