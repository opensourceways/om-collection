#!/usr/bin/python
# -*- coding: UTF-8 -*-
import smtplib
import email.mime.multipart
import email.mime.text
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


def send_email(smtp_host, smtp_port, sendAddr, password, recipientAddrs, subject='', content='', fileppt=''):
    '''
     smtp_host: 域名
     smtp_port: 端口
     sendAddr: 发送邮箱
     password: 密码
     recipientAddrs: 目标邮箱
     subject: 标题
     content: 内容
    '''
    msg = email.mime.multipart.MIMEMultipart()
    msg['from'] = sendAddr
    msg['to'] = recipientAddrs
    msg['subject'] = subject
    content = content
    txt = email.mime.text.MIMEText(content, 'plain', 'utf-8')
    msg.attach(txt)

    # 附件
    part = MIMEApplication(open(fileppt, 'rb').read())
    part.add_header('Content-Disposition', 'attachment', filename=fileppt)  # 发送文件名
    msg.attach(part)

    try:
        smtpSSLClient = smtplib.SMTP_SSL(smtp_host, smtp_port)
        loginRes = smtpSSLClient.login(sendAddr, password)  # 登录smtp服务器
        print(f"登录结果：loginRes = {loginRes}")  # loginRes = (235, b'Authentication successful')
        if loginRes and loginRes[0] == 235:
            print(f"登录成功，code = {loginRes[0]}")
            smtpSSLClient.sendmail(sendAddr, recipientAddrs, str(msg))
            print(f"mail has been send successfully. message:{str(msg)}")
            smtpSSLClient.quit()
        else:
            print(f"登陆失败，code = {loginRes[0]}")
    except Exception as e:
        print(f"发送失败，Exception: e={e}")


if __name__ == "__main__":
    try:
        subject = 'Python 测试邮件'
        content = '这是一封来自 Python 编写的测试邮件。'
        sendemail = 'xxx@163.com'
        toemail = 'xxx@huawei.com'
        smtpkey = "xxx"
        send_email('smtp.163.com', 465, sendemail, smtpkey, toemail, subject, content)
    except Exception as err:
        print(err)