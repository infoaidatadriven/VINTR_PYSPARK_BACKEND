import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
sender_title = "VINTR - Notification"
msg = MIMEText("Message text", 'plain', 'utf-8')
msg['Subject'] =  Header("Sent from python", 'utf-8')
msg['From'] = formataddr((str(Header(sender_title, 'utf-8')), 'suji.arumugam@gmail.com'))
msg['To'] = 'sujitha.arumugam@aidatadriven.com'

def sendMailtoUser(messagetext,receiver_email,receiverName):
    mailserver = smtplib.SMTP('smtp.gmail.com',587)
    # identify ourselves to smtp gmail client
    mailserver.ehlo()
    print("test")
    # secure our email with tls encryption
    mailserver.starttls()
    # re-identify ourselves as an encrypted connection
    mailserver.ehlo()
    mailserver.login('suji.arumugam@gmail.com', 'ebncoiuzyfaprukr')
    msg = MIMEText(messagetext, 'plain', 'utf-8')
    msg['Subject'] =  Header("DQ Analytics", 'utf-8')
    msg['From'] = formataddr((str(Header(sender_title, 'utf-8')), 'suji.arumugam@gmail.com'))
    msg['To'] = receiver_email
    mailserver.sendmail('suji.arumugam@gmail.com',receiver_email,msg.as_string())

    mailserver.quit()
    return ''





# def sendMailtoUser1(messagetext,receiver_email):
#     msg = MIMEMultipart('alternative')
#     msg['Subject'] = "Single sign on password"
#     msg['From'] =sender_email
#     msg['To'] = receiver_email
#     part2 = MIMEText(messagetext, 'html')
#     msg.attach(part2)
#     #context = ssl.create_default_context()
#     try:
#         with smtplib.SMTP(smtp_server, port) as server:
#             print('connectde 1')
#             server.ehlo()  # Can be omitted
#             server.starttls()
#             #server.ehlo()  # Can be omitted
#             server.login("sujitha.arumugam@aidatadriven.com", "madhu@2507")
#             print('connectde 2')
#             server.sendmail(sender_email, receiver_email, msg.as_string())
#             print('connectde 3')
#     except Exception as e:
#         print("error ",e)
#         print("unable to eend")
#     return 'True'
    


