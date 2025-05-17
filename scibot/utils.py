# LIBRARIES
import json
import requests
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from urllib.parse import urlparse
import csv
import ydb
import pandas as pd
import re

# OTHER SCRIPTS
from config import *
from sql_requests import *
from validators import *


# БАЗОВЫЕ ФУНКЦИИ
# 1.1. Функция для отправки сообщения c текстом
def send_message(chat_id, text):
    # text = text.replace("_", "\_")
    url = URL + f"sendMessage?parse_mode=markdown&chat_id={chat_id}&text={text}"
    print(url)
    requests.get(url)

# 1.2. Функция для отправки сообщения c текстом и фото
def send_photo_message(chat_id, text, image):
    url = URL + "sendPhoto"
    data = {
        'photo': image,
        'chat_id': chat_id,
        'caption': text,
        'parse_mode': 'markdown'
    }
    requests.post(url, data=data)


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


# 5.1. Функция для загрузки файл в storage
def upload_file_to_storage(file_url, participant_id, file_name):

    # Скачиваем файл
    response = requests.get(file_url)

    # Выгружаем файл с записями в хранилище
    s3.put_object(
        Body=response.content,
        Bucket=BACKET_NAME, 
        Key=file_name)

    # Возвращаем ссылку на файл
    return(EXPORT_LINK + file_name)

# 5.2. Функция для удаления файлов из object storage
def delete_file(file_url):

    parsed = urlparse(file_url)
    file_name = parsed.path.split('/')[-1]
    
    try:
        # Удаляем файл из хранилища
        response = s3.delete_object(
            Bucket=BACKET_NAME, 
            Key=file_name
        )
        print(response)
        print(f"Файл {file_name} успешно удален из хранилища")

    except Exception as e:
        print(f"Ошибка при удалении файла {file_name}: {str(e)}")
    

# 6. Функция для формирования клавитуры с вариантами опросов для опросника
def survey_choice_keyboard(study_id, exit_survey_key=False, enter_survey=None):

    # Получаем доступные опросники
    surveys, surveys_amount = get_study_surveys(study_id)

    # Учитываем настроенные входной опросник (в случае enter_survey)
    if enter_survey:
        surveys.remove(enter_survey)

    if exit_survey_key:
        callback_data = "exitsurv_"
    else:
        callback_data = "entersurv_"          

    # Формируем клавиатуру из доступных опросников
    survey_keyboard = json.dumps({
        "inline_keyboard": [
            [{"text": survey, "callback_data": callback_data + survey}] for survey in surveys
        ]
    })

    # Возвращаем клавиатуру с опросниками
    return survey_keyboard

# 7. Функция для формирования списка базовых опросов (все опросы за исключением входного и выходного)
def get_base_surveys(study_id, study_info):

    # Получаем доступные опросники
    surveys, surveys_amount = get_study_surveys(study_id)

    # Извлекаем входной и выходной опросники (при наличии)
    surveys_to_remove = []

    enter_survey = study_info.get("enterance_survey")
    if enter_survey:
        surveys_to_remove.append(enter_survey.decode('utf-8'))

    exit_survey = study_info.get("exit_survey")
    if exit_survey:
        surveys_to_remove.append(exit_survey)    

    # Удаляем входной и выходной опросы при наличии
    print(f"surveys_to_remove: {surveys_to_remove}")
    base_surveys = [s for s in surveys if s not in surveys_to_remove]

    # Формируем строку с опросами
    base_surveys_str = ", ".join(base_surveys)

    # Возвращаем и список, и строку
    return base_surveys, base_surveys_str


# 8. Функция для форматирования времени отправки / времени на прохождение при отображении в саммери
def format_sev_surveys(raw_values, measure=None):

    # Проверяем, множественный ли формат
    if '=' in raw_values:
        surveys = raw_values.split(';')
        formatted_lines = []

        for survey in surveys:
            if '=' in survey:
                key, value = survey.split('=')
                # Добавляем отступ и маркер
                if measure:
                    formatted_lines.append(f"   ◦ _{key.strip()}_: {value.strip()} {measure}")
                else:
                    formatted_lines.append(f"   ◦ _{key.strip()}_: {value.strip()}")

        # Возвращаем строку с лайнбрейком в начале и между пунктами
        return '%0A' + '%0A'.join(formatted_lines)
    else:
        # Один опрос — возвращаем как есть
        return raw_values

# 10. Функция для получения следюущего опросника
def handle_tl_for_all(base_surveys, current_tl):

    # Извлекаем имена опросов, которые уже есть в current_tl
    used_surveys = {entry.split('=')[0] for entry in current_tl}

    # Находим первый опрос из base_surveys, которого нет в current_tl
    next_survey = next((s for s in base_surveys if s not in used_surveys), None)

    # Получаем значение из первого элемента current_tl
    tl = int(current_tl[0].split('=')[1]) if current_tl else None

    return tl, next_survey


# 11. Функция для формирования времени прохождения для всех опросов
def form_one_tl_surveys(base_surveys, common_tl):
    result = []
    for survey in base_surveys:
        result.append(f"{survey}={common_tl}")
        
    return "; ".join(result)


# 12. Функция для обработки ввода времени прохождения
def general_handle_tl(chat_id, study_info, message_text):

    # Получаем текущие временне точки и интервалы
    surveys_tl = study_info.get("completion_tl")
    if surveys_tl:
        surveys_tl = surveys_tl.decode('utf-8').split('; ')

    # Получаем базовые опросники
    base_surveys = study_info["base_surveys"].decode('utf-8').split(', ')
    base_surveys_amount = len(base_surveys)

    # Если все ок с временем прохождения опросника, добавляем его
    correct, tl_surveys, message_text, keyboard, first_survey, last_survey = comp_tl_validator(message_text, base_surveys, surveys_tl)

    if correct:
        if last_survey:
            update_study(chat_id, {'status': 'comp_tl_added', 'completion_tl': tl_surveys})
            send_message(chat_id, message_text)
            return {'statusCode': 200} 
             
        elif first_survey:
            update_study(chat_id, {'status': 'one_tl_decision', 'completion_tl': tl_surveys})
        else:
            update_study(chat_id, {'completion_tl': tl_surveys})
        # Информируем об успешности этапа
        send_message_with_k(chat_id, message_text, keyboard)
        return {'statusCode': 200} 

    else:
        # Информируем об успешности этапа
        send_message(chat_id, message_text)
        return {'statusCode': 200} 
