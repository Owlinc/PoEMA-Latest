# LIBRARIES
import requests
import re
import os
import json
import pandas as pd

# OTHER SCRIPTS
from sql_requests import *
from utils import *
from db_api_requests import *
from validators import *

# HANDLER
def handler(event, context):

    message = json.loads(event['body'])
    if 'callback_query' in message:

        # Парсим колбэк
        data = message['callback_query']['data']
        chat_id = message['callback_query']['message']['chat']['id']
        study_info = get_study_info(chat_id)

        if data == "study_launch":

            if study_info['status'] != 'launched':

                # Статус 10: исследование запущено
                update_study(chat_id, {'status': 'launched'})
                send_message(chat_id, STUDY_WAS_LAUCNHED)

                # Добавление участников в базу
                participants_ids = study_info['participants_ids'].split(', ')
                for participants_id in participants_ids:
                    initaite_particip(participants_id, chat_id)
            
            else:
                send_message(chat_id, STUDY_ALREADY_LAUCNHED)

    elif 'message' in message:

        # Извлекаем текст сообщения
        if 'text' in message['message']:
            message_text = message['message']['text']
        else:
            message_text = '[no text]'

        # Извлекаем ID исследователя
        chat_id = message['message']['chat']['id']

        # По ID отправителя получаем информацию о статусе исследования
        study_info = get_study_info(chat_id)
        if 'message' in study_info.keys():
            study_status = "not_initiated"
        else:
            study_status = study_info["status"]

        # Обрабатываем стаднартные команды
        # /start
        if "/start" in message_text:
            send_message(chat_id, START_MESSAGE)
            return {'statusCode': 200}

        elif "/help" in message_text:
            send_message(chat_id, FULL_HELP_MESSAGE)
            return {'statusCode': 200}

        # /clean_study
        if "/clean_study" in message_text:
            if study_status == "not_initiated":
                send_message(chat_id, NOTHING_TO_CLEAN)
                return {'statusCode': 200}
            else:
                send_message(chat_id, SUCCESS_CLEAN)
            
            delete_study(chat_id)
            delete_survey_sql(chat_id)
            return {'statusCode': 200}

        # /get_data
        if "/get_data" in message_text:
        
            if study_status != "launched":
                send_message(chat_id, DATA_ERROR_NOT_LAUNCHED)
                return {'statusCode': 200}
            # Проверяем наличие данных, если их нет говорим об этом
            # Если есть отправляем сообщение с сслыкой на выгрузку
            data_prescence, file_url, text_message = get_beeps_data(chat_id)
            send_message(chat_id, text_message)
            return {'statusCode': 200}

        # /analyse_opens
        if "/analyse_opens" in message_text:

            if study_status != "launched":
                send_message(chat_id, OPENS_ERROR_NOT_LAUNCHED)
                return {'statusCode': 200}

            # Пытаемся провести анализ открытых
            send_message(chat_id, STARTED_OPEN_ANALYSIS)
            text_message = analyse_opens_sql(chat_id)
            
            # Информируем об успешности анализа
            send_message(chat_id, text_message)
            return {'statusCode': 200}

        # Обрабатываем сообщения в зависимости от статуса
        # Статус 1: исследование не создано, ожидается команда создания +
        if study_status == "not_initiated":

            if message_text.strip() == '/create_study':
                
                # Информируем о том, как создать исследование
                send_message(chat_id, CREATE_STUDY_INFO)
            
            elif '/create_study' in message_text:

                # Создаем исследование, если всё ок с названием
                correct, message_text, study_name = name_validator(message_text)
                if correct:
                    initaite_study(chat_id, study_name)
                    update_study(chat_id, {'status': 'initiated'})

                # Информируем об успешности этапа
                send_message(chat_id, message_text)

            else: 
            
                # Информируем о том, что можно сделать с ботом
                send_message(chat_id, HELP_MESSAGE)


        # Статус 2: исследование создано, ожидается описание +
        elif study_status == "initiated":

            # Если все ок с описание, добавляем его
            correct, message_text, description = description_validator(message_text)
            if correct:
                update_study(chat_id, {'status': 'described', 'description': description})

            # Информируем об успешности этапа
            send_message(chat_id, message_text)

        # Статус 3: описание добавлено, ожидается опросник
        elif study_status == "described":

            print("sending template...")

            # Проверяем наличие подходящего файла
            if 'document' in message['message']:

                # Проверяем корректнотсь расширения
                if message['message']['document']['file_name'].endswith(".xlsx"):
                    file_id = message['message']['document']['file_id']
                    file_url = get_file_url(file_id)
                    survey_file_url = upload_file_to_storage(file_url, chat_id, SURVEY_STRUCT_FILE_NAME.format(chat_id))
                    
                    # Проверяем корректнотсь заполнения опросника
                    correct, survey_df, response = survey_validator(survey_file_url)

                    if correct:
                        send_message(chat_id, response)
                        update_study(chat_id, {'status': 'survey_added'})
                        # Загружаем опросник в базу
                        print(survey_df)
                        print(type(survey_df))
                        upload_survey_sql(survey_df, chat_id)
                    else:
                        send_message(chat_id, SURVEY_ERROR.format(response))
                else:
                    send_message(chat_id, WRONG_SURVEY_FILE)
            else:
                send_message(chat_id, NO_FILE_SURVEY)

        # Статус 4: опросник добавлен, ожидается ограничение по времени прохождения опроса +
        elif study_status == "survey_added":

            # Если все ок с временем прохождения опросника, добавляем его
            correct, message_text, completion_tl = comp_tl_validator(message_text)
            if correct:
                update_study(chat_id, {'status': 'comp_tl_added', 'completion_tl': completion_tl})

            # Информируем об успешности этапа
            send_message(chat_id, message_text)

        # Статус 5: ограничение по времени прохождения опроса добавлено, ожидается пользовательское соглашение +
        elif study_status == "comp_tl_added":

            # Проверяем наличие подходящего файла
            if 'document' in message['message']:
                if message['message']['document']['file_name'].endswith(".pdf"):
                    file_id = message['message']['document']['file_id']
                    file_url = get_file_url(file_id)
                    agreement_file = upload_file_to_storage(file_url, chat_id, AGGR_FILE_NAME.format(chat_id))
                    send_message(chat_id, CORRECT_AGREEMENT)
                    update_study(chat_id, {'status': 'agreement_added', 'agreement_file': agreement_file})
                else:
                    send_message(chat_id, WRONG_FILE_AGREEMENT)
            else:
                send_message(chat_id, NO_FILE_AGREEMENT)

        # Статус 6: пользовательское соглашение добавлено, ожидается длительность исследования +
        elif study_status == "agreement_added":

            # Если все ок с длительностью исследования, добавляем её
            correct, message_text, duration = duration_validator(message_text)
            if correct:
                update_study(chat_id, {'status': 'duration_added', 'duration': duration})

            # Информируем об успешности этапа
            send_message(chat_id, message_text)

        # Статус 7: длительность исследования указана, ожидаются юзернеймы участников +
        elif study_status == "duration_added":

            # Если все ок с участниками, добавляем их
            correct, response = participants_validator(message_text)
            if correct:
                update_study(chat_id, {'status': 'participants_added', 'participants_ids': message_text})

            # Информируем об успешности этапа
            send_message(chat_id, response)
    
        # Статус 8: участники добавлены, ожидаются временные точки для рассылки +
        elif study_status == "participants_added":

            # Если все ок с временными точками, добавляем их
            correct, response = pt_validator(message_text)
            if correct:
                # Информируем об успешности этапа
                send_message(chat_id, response)
                update_study(chat_id, {'status': 'pt_added', 'prompting_time': message_text})
            else:
                # Информируем об успешности этапа
                send_message(chat_id, response)
                return {'statusCode': 200} 

            # Статус 9: дата окончания подключения указана, ожидается запуск исследования
            # Информируем об исследовании и предлагаем запустить его
            study_info = get_study_info(chat_id)
            send_message_with_k(
                chat_id, 
                LAUNCH_STUDY.format(
                    study_info['name'], 
                    study_info['description'].replace("_", "\_"), 
                    study_info['completion_tl'], 
                    study_info['duration'], 
                    study_info['prompting_time'],
                    study_info['participants_ids'].replace("_", "\\_")),
                launch_keyboard())

    else:
        send_message(chat_id, "Вы отправили что-то необычное. Я такое не понимаю....")
        
    return {
        'statusCode': 200,
        'body': 'Everityhing is cool: beeps are beeping, schedules are scheduling, and users are using!'
        }
        
