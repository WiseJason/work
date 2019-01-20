# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 17:23:58 2018

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
    today1 = datetime.datetime.today()+datetime.timedelta(days=-3)
    a=str(datetime.datetime(today1.year, today1.month, today1.day, 0, 0, 0))
    print(a)
    today2 = datetime.datetime.today()
    b=str(datetime.datetime(today2.year, today2.month, today2.day, 0, 0, 0))
    print(b)
    sql1="SELECT d.`house_id`, a.`room_no`, b.`true_name`, b.`mobile`, a.`principal_man`, c.`company_name`, case e.`business_type` when 1501 then '小区住宅' when 1502 then '集中公寓' when 1503 then '酒店长租' end as '房源类型', e.`estate_name` as 小区名称  from(SELECT *  from gaodu.`houseupdatelog` where `update_time` BETWEEN unix_timestamp('"+a+"')   AND unix_timestamp('"+b+"')   and `operate_type` in('已出租', '删除房源', '删除房间')) as d  INNER JOIN(select *  from gaodu.houseroom where `status`= 2   and record_status= 1) as a on a.`id`= d.`house_id`  LEFT JOIN `gaodudata`.`customer` as b on a.`customer_id`= b.`id`  LEFT JOIN `gaodustore`.`storecompanymembers` as c on a.`customer_id`= c.`customer_id`  LEFT JOIN gaodu.`houseresource` as e on a.`resource_id`= e.`id`"
    sqlcmd1=sql1
    data1=pd.read_sql(sqlcmd1,dbconn)
    sql2="SELECT `house_id`, `update_man`, `operate_type`  from gaodu.`houseupdatelog` where `update_time` BETWEEN unix_timestamp('"+a+"')   AND unix_timestamp('"+b+"')   and `operate_type` in ('上架操作', '修改房间', '唤醒新增房间', '重新上架', '重新出租房间', '系统上架', '更新房源操作','恢复删除')"
    sqlcmd2=sql2
    data2=pd.read_sql(sqlcmd2,dbconn)
    print(data1)
    print(data2)
    data=pd.merge(data2,data1,how="left")
    del data["house_id"]
    data=data.dropna(subset=["room_no"])
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    filename=yesterday+".xlsx"
    data.to_excel(filename,encoding = "gbk",index=None)
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
    email_content = yesterday+"重新上架数据"
    email_content_html = """
    <p>Python 邮件发送...</p>
    <p><a href="http://www.runoob.com">菜鸟教程链接</a></p>
    <p>图片：</p>
    <p><img src="cid:image1"></p>
    """
    
    email_subject =yesterday+"重新上架数据据"
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
  

