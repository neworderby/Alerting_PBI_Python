# нужные библиотеки
import requests
import requests_ntlm
from requests_ntlm import HttpNtlmAuth
import smtplib
import base64
import os
import pandas as pd


auth = HttpNtlmAuth("Login", "Password")

def folder_report_info(folder_id):
    url = f'http://server//reports/api/v2.0/Folders({folder_id})/CatalogItems'
    response = requests.get(url, auth=auth, verify=False)
    resp_json = response.json()
    df_folder = pd.DataFrame(resp_json['value'])
    df_folder['Folder'] = (df_folder["Path"]
                                         .str.split('/', 3).str[2])
    return df_folder

df_1 = folder_report_info('ID_Folder') #Id папки PBI RS
df_2 = folder_report_info('ID_Folder') #Id папки PBI RS
df_3 = folder_report_info('ID_Folder') #Id папки PBI RS
df_info_ = [df_1,df_2,df_3] #Объединение в единый датафрейм
df_info_ = pd.concat(df_info_) #Объединение в единый датафрейм


http_1 = 'http://server//reports/api/v2.0/PowerBIReports('
http_2 = ')/CacheRefreshPlans'
url = [http_1 + x + http_2 for x in df_info_['Id']] #из предыдущей таблицы с основной информацией об отчетах.

df_info_refresh = [] #объявляем пустой список, в который будем складывать записи по api

#Собираем данные для фрейма
for i in range(len(url)): #цикл по ранее полученным ссылкам на отчеты
    response = requests.get(str(url[i]), auth=auth, verify=False) #подключение по api
    resp_json = response.json() #складываем в json
    df_refresh = pd.DataFrame(resp_json['value']) #складываем в фрейм
    for j in range(len(df_refresh)): ##для чтения всех записей фреймов
        df_info_refresh.append(df_refresh) #добавляем очередную запись в итогоый список
df_info_refresh = pd.concat(df_info_refresh) #склеииваем записи в единый фрейм

#Обработка фрейма
df_info_refresh = ( 
    df_info_refresh[['Id', 'Owner', 'Description', 
                             'CatalogItemPath', 'EventType',
                             'ScheduleDescription', 'LastRunTime', 
                             'LastStatus', 'ModifiedBy','ModifiedDate']]) #объявляем все строки, которые нужны для выгрузки

df_info_refresh = df_info_refresh.drop_duplicates(keep='last') #удаляем дубликаты (не последние записи)
df_info_refresh["ReportName"] = (df_info_refresh["CatalogItemPath"]
                                         .str.split('/', -1).str[-1]) #создаем столбец с названием отчета



from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib

df_failed_refresh = (df_info_refresh[['Owner','ReportName','CatalogItemPath',
                                              'Description','LastRunTime','LastStatus']]
                     [df_info_refresh['LastStatus']!='Completed Data Refresh'])



recipients = ['mail1.ru','mail2.ru'] #Список получателей
emaillist = [elem.strip().split(',') for elem in recipients]
msg = MIMEMultipart()
msg['Subject'] = "Daily Failed Refresh PBI Reporting" #Тема письма
FROMADDR = "you@gmail.com" #С какого почтового адреса будет отправка gmail.com
LOGIN    = FROMADDR #Логин/Отправитель
PASSWORD = "yourcode" #полученный 16-значный код


html = """\
<html>
  <head></head>
  <body>
    {0}
  </body>
</html>
""".format(df_failed_refresh.to_html()) #Формирование фрейма для отображения в письме

part1 = MIMEText(html, 'html')
msg.attach(part1)

server = smtplib.SMTP('smtp.gmail.com', 587) #Отправка письма
server.set_debuglevel(1)
server.set_debuglevel(1)
server.ehlo()
server.starttls()
server.login(LOGIN, PASSWORD)
server.sendmail(FROMADDR, emaillist , msg.as_string())
server.quit() #Закрытие подключения