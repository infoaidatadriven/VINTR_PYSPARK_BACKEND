from datetime import datetime, date
import os
import smtplib
import pathlib

from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
from email.mime.base import MIMEBase

filename = str(date.today()) + ".png"
dir = pathlib.Path(__file__).parent.absolute()
folder = r"/results/"
path = str(dir) + folder + filename

# path to header image
folder_img = r"/img/"
path_img =  str(dir) + folder_img + "header_img.png"


def send_email_new(path_plot, from_mail,  to_mail,MailTemplatePath, FileNameText,FileSubject):
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = FileSubject
    msgRoot['From'] = from_mail
    msgRoot['To'] = to_mail
    msgRoot.preamble = FileSubject

    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)

    msgText = MIMEText(FileSubject)
    msgAlternative.attach(msgText)

    HTMLFile = open(MailTemplatePath, "r") 
    string = HTMLFile.read()
    string = string.replace("'/results/filename>'", FileNameText+" ")
    # We reference the image in the IMG SRC attribute by the ID we give it below
    #msgText = MIMEText('<b>Some <i>HTML</i> text</b> and an image.<br><img src="cid:image1"><br>Nifty!', 'html')
    msgText = MIMEText(string, 'html')
    msgAlternative.attach(msgText)

    # This example assumes the image is in the current directory
    fp = open(path_plot, 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()

    # Define the image's ID as referenced above
    msgImage.add_header('Content-ID', '<image1>')
    msgRoot.attach(msgImage)

    fp1 = open("landing-bg.jpg", 'rb')
    msgImage1 = MIMEImage(fp1.read())
    fp1.close()

    # Define the image's ID as referenced above
    msgImage1.add_header('Content-ID', '<imageland>')
    msgRoot.attach(msgImage1)


    fp2 = open("VINTR_logo.jpg", 'rb')
    msgImage2 = MIMEImage(fp2.read())
    fp2.close()

    # Define the image's ID as referenced above
    msgImage2.add_header('Content-ID', '<imagelogo>')
    msgRoot.attach(msgImage2)



    # Send the email via our own SMTP server
    mailserver = smtplib.SMTP('smtp.gmail.com',587)
    # identify ourselves to smtp gmail client
    mailserver.ehlo()
    # secure our email with tls encryption
    mailserver.starttls()
    # re-identify ourselves as an encrypted connection
    mailserver.ehlo()
    mailserver.login('suji.arumugam@gmail.com', 'ebncoiuzyfaprukr')
    
    mailserver.sendmail(from_mail, to_mail, msgRoot.as_string())
    mailserver.quit()
    print("sent the message to user"+ to_mail)
    return 'true'

def send_email_withoutChart(path_plot, from_mail,  to_mail,MailTemplatePath, FileNameText,FileSubject):
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = FileSubject
    msgRoot['From'] = from_mail
    msgRoot['To'] = to_mail
    msgRoot.preamble = FileSubject

    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)

    msgText = MIMEText(FileSubject)
    msgAlternative.attach(msgText)

    HTMLFile = open(MailTemplatePath, "r") 
    string = HTMLFile.read()
    string = string.replace("'/results/filename>'",  FileNameText+" ")
    # We reference the image in the IMG SRC attribute by the ID we give it below
    #msgText = MIMEText('<b>Some <i>HTML</i> text</b> and an image.<br><img src="cid:image1"><br>Nifty!', 'html')
    msgText = MIMEText(string, 'html')
    msgAlternative.attach(msgText)

    

    fp1 = open("landing-bg.jpg", 'rb')
    msgImage1 = MIMEImage(fp1.read())
    fp1.close()

    # Define the image's ID as referenced above
    msgImage1.add_header('Content-ID', '<imageland>')
    msgRoot.attach(msgImage1)


    fp2 = open("VINTR_logo.jpg", 'rb')
    msgImage2 = MIMEImage(fp2.read())
    fp2.close()

    # Define the image's ID as referenced above
    msgImage2.add_header('Content-ID', '<imagelogo>')
    msgRoot.attach(msgImage2)



    # Send the email via our own SMTP server
    mailserver = smtplib.SMTP('smtp.gmail.com',587)
    # identify ourselves to smtp gmail client
    mailserver.ehlo()
    # secure our email with tls encryption
    mailserver.starttls()
    # re-identify ourselves as an encrypted connection
    mailserver.ehlo()
    mailserver.login('suji.arumugam@gmail.com', 'ebncoiuzyfaprukr')
    
    mailserver.sendmail(from_mail, to_mail, msgRoot.as_string())
    mailserver.quit()
    print("sent the message to user" + to_mail)
    return 'true'
