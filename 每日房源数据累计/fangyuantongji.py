# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 14:15:24 2018

@author: Administrator
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
    sql="SELECT (CASE `city_code` WHEN 001009001 THEN '上海'WHEN 001001 THEN '北京'WHEN 001010001 THEN '南京'WHEN 001010013 THEN '苏州'WHEN 001011001 THEN '杭州'WHEN 001016001 THEN '郑州'WHEN 001017001 THEN '武汉' WHEN 001019001 THEN '广州'WHEN 001019002 THEN '深圳'WHEN 001002001 THEN '天津' END ) AS '城市',  count(`city_code`)  as 可租房源 FROM `gaodu`.`houseroom` where `status` =2 and record_status=1 GROUP BY `city_code`  "
    sqlcmd=sql
    data=pd.read_sql(sqlcmd,dbconn)#可租房源
    data.insert(0,"日期","")
    data["日期"]=time.strftime("%Y/%m/%d")
    data.to_csv("fangyuan.csv",mode='a+',encoding="gbk",index=False)
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
 
    to_addr_list = ["bianxiezhong@hizhu.com","chenyuchao@hizhu.com"]       # 收件地址
 
    email_content = "每日房源累计"
    email_content_html = """
    <p>Python 邮件发送...</p>
    <p><a href="http://www.runoob.com">菜鸟教程链接</a></p>
    <p>图片：</p>
    <p><img src="cid:image1"></p>
    """
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)  
    email_subject ="房源数据"
    email_from = "卞协忠"
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    source_path="fangyuan.csv"
    part_name ="fangyuan.csv"
    getdata()
    current_time = datetime.datetime.now()   #当前时间date
    if current_time.weekday()==0:
        print("start")
        email_obj = get_email_obj(email_subject, email_from, to_addr_list)
        attach_content(email_obj, email_content)
        attach_part(email_obj, source_path, part_name)
        send_email(email_obj, email_host, host_port, from_addr, pwd, to_addr_list)
        print("end")