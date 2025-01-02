# LIBRARIES
import json
import requests
from datetime import datetime, timedelta
from io import StringIO, BytesIO
import csv
import ydb
import pandas as pd
import re

# OTHER SCRIPTS
from config import *
# from sql_requests import *


# БАЗОВЫЕ ФУНКЦИИ
# 1. Функция для отправки сообщения c текстом
def send_message(chat_id, text):
    # text = text.replace("_", "\_")
    url = URL + f"sendMessage?parse_mode=markdown&chat_id={chat_id}&text={text}"
    print(url)
    requests.get(url)


# 2. Функция для отправки сообщения c текстом и клавиатурой
def send_message_with_k(chat_id, text, keyboard):
    url = URL + f"sendMessage?text={text}&chat_id={chat_id}&parse_mode=markdown&reply_markup={keyboard}"  
    print(url)
    requests.get(url)


# 3. Функция для отправки документа
def send_file(chat_id, text, df_to_sent):

    # Запись DataFrame в строковой буфер
    print(df_to_sent)
    buf = BytesIO()
    csv_buffer = StringIO()

    df_to_sent.to_csv(csv_buffer)
    csv_buffer.seek(0)

    buf.write(csv_buffer.getvalue().encode())
    buf.seek(0)
    buf.name = f'data_report.csv'
    csv_buffer.name = f'data_report.csv'

    # text = text.replace("_", "\_")
    url = URL + f"sendDocument"
    print(buf)
    data = {
        'document': csv_buffer,
        'chat_id': chat_id,
        'caption': text,
        'parse_mode': 'Markdown'
    }
    print(url)
    requests.post(url, data=data)


# 4. Функция для получения ссылки на файл
def get_file_url(file_id):
    url = FILE_DATA_URL.format(TG_TOKEN, file_id)
    json_results = requests.get(url)
    file_path = json_results.json()['result']['file_path']
    file_url = FILE_DOWNLOAD_URL.format(TG_TOKEN, file_path)
    print(file_url)
    return file_url


# 5. Функция для загрузки файл в storage
def upload_file_to_storage(file_url, participant_id, file_name):

    # Скачиваем файл
    response = requests.get(file_url)
    print(response)

    # Выгружаем файл с записями в хранилище
    s3.put_object(
        Body=response.content,
        Bucket=BACKET_NAME, 
        Key=file_name)

    # Возвращаем ссылку на файл
    return(EXPORT_LINK + file_name)
    
