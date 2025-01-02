# LIBRARIES
import requests
import re
import os
import json
import pandas as pd
from datetime import datetime, date
import time
import threading

# OTHER SCRIPTS
from sql_requests import *
from utils import *
from config import *
from db_api_requests import *

# HANDLER
def handler(event, context):
     
    try:
        message = json.loads(event['body'])
    except Exception as error:
        message = False

    if  message:
        # Обработка колбэков
        if 'callback_query' in message:

            # Парсим колбэк
            data = message['callback_query']['data']
            chat_id = message['callback_query']['message']['chat']['id']
            username = message['callback_query']['message']['chat']['username']

            # Инфо об исследовании и участии
            particip_info = get_particip_info(username)
            study_info = get_study_info(particip_info['study_id'])

            # Извлекаем из колбэка все необходимые данные
            callback_data = message['callback_query']['data'].split('_')

            # Развилка по типам колбэков
            if callback_data[0] == "particip":

                # Подключение к исследованию
                if callback_data[1] == "join":

                    # Записываем ID респа в базу
                    write_particip_id(chat_id, username)

                    # Обновляем статус: ожидаем согласие
                    update_particip(username, "agreement waiting")

                    # Отправлеям пользователськое соглашение
                    send_file(chat_id, AGREEMENT_TITLE, study_info['agreement_file'])
                    
                    # Отправлеям клавиатур с согласием
                    send_message_with_k(
                        chat_id, 
                        AGREEMENT_PROMPT, 
                        agreement_keyboard())

                # Участие в исследовании
                elif callback_data[1] == "agree":

                    # Удаляем предыдщие записи
                    delete_beeps(chat_id)

                    # Обновляем статус: участие
                    update_particip(username, "participating")
                    send_message(chat_id, PARTICIP_INFO_MESSAGE.format(
                        study_info['completion_tl'], 
                        study_info['duration'],
                        study_info['prompting_time']
                    ))
                    
                    # Готовим данные для загрузки в БД
                    # Получаем длину опросника
                    survey_len = get_survey_len(particip_info['study_id'])
                    # Заготовка для бипов в рамках исследования
                    beep_dicts = {}
                    # Проходимя по каждой временной точке
                    time_points = study_info['prompting_time'].split(",")
                    for time_to_send in time_points:
                        
                        # Преобразуем строковую дату отправки в dt
                        time_to_send = time_to_send.strip()
                        time_to_send = datetime.strptime(time_to_send, "%H:%M").time()
                        time_to_send = datetime.combine(date.today(), time_to_send)

                        # Формируем словари дли рассылки бипов
                        beep_dicts = form_beep_dicts(
                            chat_id, particip_info['study_id'], survey_len, study_info['duration'], time_to_send, study_info['completion_tl'])

                        # Загружаем данные в БД
                        upload_beeps(beep_dicts)
                        return {'statusCode': 200} 

                # Исключение из исследования
                elif callback_data[1] == "disagree":
                    send_message(chat_id, BYE_BYE_MESSAGE)
                    delete_particip(username)
                    return {'statusCode': 200}

            else:
                # Работаем по обновлению бипа
                beep_id = callback_data[0]
                response = callback_data[1]
                question_id = callback_data[2]
                message_id = message['callback_query']['message']['message_id']
                user_id = message['callback_query']['from']['id']

                # Обновляем сообщением – подставляем новый бип
                update_message(user_id, message_id, question_id)

                # Обновляем данные о бипе в таблице
                update_beep_db(beep_id, response, message_id)

            return {'statusCode': 200}

    
        # Обработка сообщений
        elif 'message' in message:

            # Извлекаем chat_id 
            chat_id = message['message']['chat']['id']

            # Извлекаем username участника
            username = message['message']['chat']['username']

            # Извлекаем текст сообщения
            if 'text' in message['message']:
                message_text = message['message']['text']
            else:
                message_text = '[no text]'

            # По ID отправителя получаем информацию о статусе исследования
            particip_info = get_particip_info(username)
            if 'message' in particip_info.keys():
                particip_status = "not_invited"
            else:
                particip_status = particip_info["status"]
                study_id = particip_info["study_id"]
                study_info = get_study_info(study_id)

            ## ОБРАБОТКА КОМАНД
            # /start
            if '/start' in message_text:
                send_message(chat_id, START_MESSAGE)
                return {'statusCode': 200}

            elif '/help' in message_text:
                send_message(chat_id, HELP_MESSAGE)
                return {'statusCode': 200}
            
            elif '/leave_study' in message_text:
                if particip_status == "not_invited":
                    send_message(chat_id, NOWHERE_TO_LEAVE_MESSAGE)
                    return {'statusCode': 200}
                else:
                    send_message(chat_id, BYE_BYE_MESSAGE)
                    delete_particip(username)
                    return {'statusCode': 200}

            # /find_my_study
            elif '/find_my_study' in message_text:
                if particip_status == "not_invited":
                    send_message(chat_id, NOT_INVITED_MESSAGE)
                if particip_status == "ended":
                    send_message(chat_id, ALREAY_PARTICIPATED_INFO)                    
                elif particip_status == "awaited":

                    # Отправляем клаву для подклчения к исследованию
                    send_message_with_k(
                        chat_id, 
                        STUDY_INFO_MESSAGE.format(study_info['name'], study_info['description']), 
                        join_keyboard())

                elif particip_status == "participating" or  particip_status == "agreement waiting":
                    send_message(chat_id, ALREADY_HERE_MESSAGE)

            if particip_status == "answers_expected":

                # Получаем информацию о бипе, который ожидает ответа
                beep_id, question_id, message_id = get_beep_to_write(chat_id)

                # Обновляем сообщение – подставляем новый бип
                update_message(chat_id, message_id, question_id)

                # Обновляем данные о бипе в таблице
                update_beep_db(beep_id, message_text, message_id)


    else:
        # Отправляем бипы по расписанию
        beeps_to_send_df = get_user_beeps()
        sent_to_id = []
        if beeps_to_send_df:
            for row in beeps_to_send_df[0].rows:
                if row['participant_id'] not in sent_to_id:
                    participant_id, question_text, keyboard = prepare_beep(row)
                    send_beep(participant_id, question_text, keyboard)
                    sent_to_id.append(row['participant_id'])
                    # Говорим системе, что от респондента ожидаются ответы
                    update_particip(get_username(row['participant_id']), "answers_expected")
                    
        
        # Работаем с истекшими бипами
        users_to_check = check_expired_beeps()
        if users_to_check:
            for user_id in users_to_check:
                # Проверяем закончилось ли исследование
                if check_study_end(user_id):
                    # Обновляем статус: завершил участия
                    update_particip(get_username(user_id), "ended")
                    # Если закончилось, то завершаем его
                    time.sleep(10)
                    send_message(user_id, THE_END_MESSAGE)
                # Если исследование не закончилось, то просто говорим системе о том, что опрос окончился
                else:
                    update_particip(get_username(user_id), "participating")

            
    return {
        'statusCode': 200,
        'body': 'Everityhing is cool: beeps are beeping, schedules are scheduling, and users are using!',
    }
