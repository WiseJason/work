# -*- coding: utf-8 -*-
"""
Created on Thu Jun 28 16:39:51 2018

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

def classification(x,y):
    if (x=="上海" or x=="北京" or x=="苏州" or x=="天津") and (y=="房东版发布" or y=="PC版发布" or y=="BD" or y=="蘑菇租房"):
        return "总部信审品控"
    if y=="个人发布":
        return "总部信审品控"
    if (x=="杭州" or x=="深圳" or x=="广州" )  and (y=="PC版发布" or y=="房东版发布" or y=="BD" or y=="蘑菇租房" or y=="爱上租"):
        return "杭分运营"
    if (x=="南京" or x=="郑州" or x=="武汉") and (y=="PC版发布" or y=="房东版发布" or y=="BD" or y=="蘑菇租房"):
        return "宁分运营"
    if y in ("搜房","58","赶集","链家网","中原地产","Q房网","爱屋吉屋","我爱我家","品牌公寓"):
        return ("陈育超/刘玥")
def getdata():
    today1 = datetime.datetime.today()+datetime.timedelta(days=-2)
    a=str(datetime.datetime(today1.year, today1.month, today1.day, 18, 0, 0))
    today2= datetime.datetime.today()+datetime.timedelta(days=-1)
    b=str(datetime.datetime(today2.year, today2.month, today2.day, 18, 0, 0))
    print(a)
    print(b)
    sql="SELECT a.`city_id` as '城市',a.`estate_name` as '小区名称', a.`room_id` as '房间编号',       a.`agent_company_name` as '品牌',       a.`info_resource` as '来源',       a.`mobile` as '租客手机',       a.`owner_mobile` as '房东手机',       a.`owner_name` as '房东姓名',       a.`charge_man` as '房源负责人',       a.`principal_man` as '房东负责人',       a.`updata_man` as '记录人',       from_unixtime(a.`update_time`) as '记录时间',       a.`memo` as '操作',       a.`customer_id` as 房客ID,       a.`owner_id` as 房东ID,       a.`gaodu_platform`,case c.`business_type` when 1501 then '小区住宅' when 1502 then '集中公寓' when 1503 then '酒店长租' end as '业务类型'  FROM gaodu.`houserentercall` as a LEFT JOIN gaodu.`houseroom` as b on a.`room_id` =b.`room_no` LEFT JOIN gaodu.`houseresource` as c on b.`resource_id` =c.`id`  where a.`is_read`= 1   and a.`call_time` BETWEEN unix_timestamp('"+a+"')   AND unix_timestamp('"+b+"')   and a.`updata_man`<> ''   and a.`memo`<> ''   and a.`memo` not in ('更新房源—') GROUP BY a.`mobile`,         a.`room_id`,         a.`owner_mobile`"
    sqlcmd=sql
    data=pd.read_sql(sqlcmd,dbconn)
    data["城市"]=data["城市"].replace("001009001","上海")
    data["城市"]=data["城市"].replace("001001","北京")
    data["城市"]=data["城市"].replace("001011001","杭州")
    data["城市"]=data["城市"].replace("001010001","南京")
    data["城市"]=data["城市"].replace("001019002","深圳")
    data["城市"]=data["城市"].replace("001019001","广州")
    data["城市"]=data["城市"].replace("001016001","郑州")
    data["城市"]=data["城市"].replace("001010013","苏州")
    data["城市"]=data["城市"].replace("001002001","天津")
    data["城市"]=data["城市"].replace("001017001","武汉")
    data.insert(1,"渠道","录音")
    data.insert(14,"责任人","")
    data.insert(12,"问题描述","")
    data.insert(12,"质检结果","")
    data.insert(12,"备注","")
    data['操作'],data['问题描述']=data['操作'].str.split("—",1).str
    data["责任人"]=pd.Series(list(map(lambda x, y:classification(x,y),data["城市"],data["来源"])))   
    data=data[["城市","渠道","房间编号","业务类型","小区名称","品牌","来源","租客手机","房东手机","房东姓名","房源负责人","房东负责人","记录人","记录时间","操作","问题描述","责任人","质检结果","备注","房客ID","房东ID","gaodu_platform"]]
    print(data.info())
    data1=data.loc[data["gaodu_platform"].isin(["14","15"])]
    data2=data.loc[~data["gaodu_platform"].isin(["14","15"])]
    data1.rename(columns={"租客手机":"房东手机",
                     "房东手机":"租客手机",
                     "房东ID":"房客ID",
                     "房客ID":"房东ID"
                    },inplace = True)
    data1=data1[["城市","渠道","房间编号","业务类型","小区名称","品牌","来源","租客手机","房东手机","房东姓名","房源负责人","房东负责人","记录人","记录时间","操作","问题描述","责任人","质检结果","备注","房客ID","房东ID","gaodu_platform"]]
    data=pd.concat([data2,data1],axis=0)
    del data["房客ID"]
    del data["gaodu_platform"]
    data.drop_duplicates(["租客手机","房东手机","房间编号"],inplace = True)
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
 
    to_addr_list = ["ops@hizhu.com"]       # 收件地址
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    email_content = yesterday+"录音数据"
    email_content_html = """
    <p>Python 邮件发送...</p>
    <p><a href="http://www.runoob.com">菜鸟教程链接</a></p>
    <p>图片：</p>
    <p><img src="cid:image1"></p>
    """
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)  
    email_subject =yesterday+"录音数据"
    email_from = "卞协忠"
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1) 
    yesterday=str(today-oneday)
    source_path=yesterday+".xlsx"
    part_name =yesterday+".xlsx"
    print ("START")
    getdata()
    print("finish")
    email_obj = get_email_obj(email_subject, email_from, to_addr_list)
    attach_content(email_obj, email_content)
    attach_part(email_obj, source_path, part_name)
    send_email(email_obj, email_host, host_port, from_addr, pwd, to_addr_list)
     
