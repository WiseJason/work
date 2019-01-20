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

dbconn=pymysql.connect(
host="rr-bp1refgx3467t7y54o.mysql.rds.aliyuncs.com",
database="gaodu",
user="gdroot",
password="gdroot",
port=3306,
charset='utf8'
)
def getdata():
    sql1="SELECT b.`city_code`as '城市',c.`agent_company_name` as '品牌',c.`true_name` as'商家姓名' ,c.`mobile` as '商家电话' ,b.`customer_id`    FROM gaodu.`openapishow` as a LEFT JOIN gaodu.houseroom as b on a.`room_no`=b.`room_no` LEFT JOIN `gaodudata`.`customer` as c on b.`customer_id` =c.`id`   WHERE a.`is_push` =1  and a.`third_type` =1  GROUP BY b.`city_code` ,c.`agent_company_name` ,c.`true_name` ,b.`customer_id` "
    today1 = datetime.datetime.today()+datetime.timedelta(days=-1)
    a=str(datetime.datetime(today1.year, today1.month, today1.day, 0, 0, 0))
    print(a)
    today2= datetime.datetime.today()
    b=str(datetime.datetime(today2.year, today2.month, today2.day, 0, 0, 0))
    print(b)
    c="2018-05-27 00:00:00"
    sql2="SELECT a.mobile,a.room_id,a.owner_id as `customer_id`  FROM `gaodu`.`houserentercall` AS A left join gaodu.`houseroom` AS B on B.`room_no`= A.`room_id` left join gaodu.`houseresource` AS C on B.`resource_id`= C.`id` left JOIN `gaodudata`.`customer` as d on a.`customer_id`= d.id where call_time BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"') and a.big_code= '4008170019'  GROUP BY a.mobile,a.room_id,b.customer_id"
    sql3="SELECT a.mobile,a.room_id,a.owner_id as `customer_id`  FROM `gaodu`.`houserentercall` AS A left  join gaodu.`houseroom` AS B on B.`room_no`= A.`room_id` left JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id` left JOIN `gaodudata`.`customer` as d on a.`customer_id`= d.id where call_time BETWEEN unix_timestamp('"+c+"') AND unix_timestamp('"+b+"') and a.big_code= '4008170019' GROUP BY a.mobile,a.room_id,b.customer_id"
    sql4="SELECT a.customer_mobile as `mobile`,b.room_no as `room_id`, a.owner_id as `customer_id` FROM `gaodu`.`housereservecall` as A left JOIN gaodu.`houseroom` AS B on B.id= A.`room_id` left JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id` left JOIN `gaodudata`.`customer` AS D on d.`id` = A.`customer_id`   WHERE A.`create_time` BETWEEN unix_timestamp('"+a+"') AND unix_timestamp('"+b+"')  and a.`gaodu_platform` =55 and a.`status` in(1,2)"
    sql5="SELECT a.customer_mobile as `mobile`,b.room_no as `room_id`, a.owner_id as `customer_id` FROM `gaodu`.`housereservecall` as A left JOIN gaodu.`houseroom` AS B on B.id= A.`room_id` left JOIN gaodu.`houseresource` AS C on B.`resource_id`= C.`id` left JOIN `gaodudata`.`customer` AS D on d.`id` = A.`customer_id`   WHERE A.`create_time` BETWEEN unix_timestamp('"+c+"') AND unix_timestamp('"+b+"')  and a.`gaodu_platform` =55 and a.`status` in (1,2)"
    sqlcmd1=sql1
    data1=pd.read_sql(sqlcmd1,dbconn)
    data1["customer_id"]=data1["customer_id"].str.upper()
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    filename=yesterday+"总数.xlsx"
    data1.to_excel(filename,encoding = "gbk",index=None)
    
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
    data2["customer_id"]=data2["customer_id"].str.upper()
    yesterday=str(today-oneday)
    filename=yesterday+"lianxi.xlsx"    
    data2.to_excel(filename,encoding = "gbk",index=None)
    sqlcmd4=sql4
    data4=pd.read_sql(sqlcmd4,dbconn)
    data4["customer_id"]=data4["customer_id"].str.upper()
    data6=pd.concat([data2,data4], axis=0)
    data6.drop_duplicates(["mobile","room_id","customer_id"],'first',inplace = True)
    del data6['mobile']
    del data6['room_id']
    filename=yesterday+"lianxi1.xlsx"    
    data6.to_excel(filename,encoding = "gbk",index=None)
    data6.insert(1,"当日","")
    data6=data6.groupby(["customer_id"]).count().reset_index()
   
    data6=pd.DataFrame(data6)
    
    sqlcmd3=sql3
    data3=pd.read_sql(sqlcmd3,dbconn)
    data3["customer_id"]=data3["customer_id"].str.upper()
    sqlcmd5=sql5
    data5=pd.read_sql(sqlcmd5,dbconn)
    data5["customer_id"]=data5["customer_id"].str.upper()
    filename=yesterday+"yuyue.xlsx" 
    data5.to_excel(filename,encoding = "gbk",index=None)
    data7=pd.concat([data3,data5], axis=0)
    data7.drop_duplicates(["mobile","room_id","customer_id"],'first',inplace = True)
    del data7['mobile']
    del data7['room_id']
    data7.insert(1,"累计","")
    data7=data7.groupby(["customer_id"]).count().reset_index()
    data7=pd.DataFrame(data7)
    data8=pd.merge(data1,data6, how="left")
    data8.fillna(0,inplace=True)
    data8["当日"]=data8["当日"].astype(int)
    data9=pd.merge(data8,data7, how="left")
    data9.fillna(0,inplace=True)
    data9["累计"]=data9["累计"].astype(int)
    del data9['customer_id']
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    filename=yesterday+".xlsx"
    data9.to_excel(filename,encoding = "gbk",index=None)


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
    from_addr = "bianxiezhong@hizhu.com"                # 发件地址
    pwd = "44070423Xbj"                  # 发件地址的授权码，而非密码
 
    # （163邮箱）
    # email_host = "smtp.163.com"             # smtp 邮件服务器
    # host_port = 465                         # smtp 邮件服务器端口：SSL 连接
    # from_addr = "发件地址"                  # 发件地址
    # pwd = "授权码"                    # 发件地址的授权码，而非密码
 
    to_addr_list = ["bianxiezhong@hizhu.com"]       # 收件地址
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    email_content = yesterday+"支付宝链接数据"
    email_content_html = """
    <p>Python 邮件发送...</p>
    <p><a href="http://www.runoob.com">菜鸟教程链接</a></p>
    <p>图片：</p>
    <p><img src="cid:image1"></p>
    """
    
    email_subject =yesterday+"支付宝链接数据"
    email_from = "卞协忠"
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    source_path=yesterday+".xlsx"
    part_name =yesterday+".xlsx"
    getdata()
    email_obj = get_email_obj(email_subject, email_from, to_addr_list)
    attach_content(email_obj, email_content)
    attach_part(email_obj, source_path, part_name)
    send_email(email_obj, email_host, host_port, from_addr, pwd, to_addr_list)
  




