# LIBRARIES
import requests
import re
import os
import json
import pandas as pd
from datetime import datetime, date
import time
import random

# OTHER SCRIPTS
from sql_requests import *
from utils import *
from config import *
from validators import *

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
            question_text = message['callback_query']['message']['text']
            formatting = message['callback_query']['message'].get('entities') 

            # Извлекаем из колбэка все необходимые данные
            callback_data = message['callback_query']['data'].split('_')

            # Применяем форматирование к сообщению
            if formatting:
                question_text = apply_formatting(question_text, formatting)

            # Инфо об исследовании и участии
            particip_info = get_particip_info(username)
            if not particip_info and callback_data[0] != "delete":
                 send_message(chat_id, NON_ACTIVE_KEYBOARD)
                 return {'statusCode': 200} 
            elif particip_info:
                study_info = get_study_info(particip_info['study_id'].decode('utf-8'))

            if not particip_info:
                particip_status = "not_invited"
            else:
                particip_status = particip_info["status"].decode('utf-8')
                previous_status = particip_info.get('previous_status')
                if previous_status is not None:
                    previous_status = previous_status.decode('utf-8')

            # Развилка по типам колбэков
            # Обработка вопросов типа multiple choice
            if callback_data[0] == "mc":

                # Получаем номер бипа
                beep_id = callback_data[1]

                # Получаем информацию о бипе
                question_id, message_id, question_type = get_beep_data(beep_id)

                # Получаем клавиатуру
                mc_keyboard = message['callback_query']['message']['reply_markup']['inline_keyboard']

                # Получаем действие: отправка выбора, очистка выбора или выбор
                action = callback_data[2]

                # Отправка выбора
                if action == "send":

                    # Парсим клавиатуру
                    chose_anything, chosen_options = extract_selected_options(mc_keyboard)

                    # Если что-то выбрано – идем дальше
                    if chose_anything:
                        update_message(chat_id, message_id, question_id)

                    # Если ничего не выбрано – отправляем сообщение об этом 
                    else:
                        send_message(chat_id, NO_CHOSEN_OPTIONS)

                # Очистка выбора
                elif action == "clean":

                    # Парсим клавиатуру
                    chose_anything, chosen_options = extract_selected_options(mc_keyboard)

                    # Если есть что очистить – очищаем
                    if chose_anything:
                        mc_keyboard = clean_selected_options(mc_keyboard)
                    else:
                        mc_keyboard = json.dumps({"inline_keyboard": mc_keyboard})

                    # Обновляем текст сообщения
                    question_text, changes = handle_mc_clean_text(chose_anything, question_text)

                    if not changes:
                        return {'statusCode': 200}

                    # Обновляем сообщение – подставляем новую клавиатуру
                    edit_question_message(chat_id, message_id, question_text, mc_keyboard)

                    # Обновляем данные о бипе в таблице
                    update_beep_db(beep_id, "[empty]", message_id)
                    
                # Выбор
                else:

                    # Информируем о начале работы
                    edit_question_message(chat_id, message_id, LOADING_TEXT)

                    # Обновляем текст сообщения
                    question_text = remove_mc_clean_text(question_text)

                    # Обновляем сообщение – подставляем новую клавиатуру
                    raw_keyboard, mc_keyboard = handle_mc_choice(data, mc_keyboard)
                    edit_question_message(chat_id, message_id, question_text, mc_keyboard)

                    # Парсим клавиатуру
                    chose_anything, chosen_options = extract_selected_options(raw_keyboard)

                    # Обновляем данные о бипе в таблице
                    update_beep_db(beep_id, chosen_options, message_id)
                
                return {'statusCode': 200}


            elif callback_data[0] == "particip":

                # Подключение к исследованию
                if callback_data[1] == "join":

                    # Проверка работоспособности клавиатуры
                    if particip_status != "awaited":
                        send_message(chat_id, NON_ACTIVE_KEYBOARD)
                        return {'statusCode': 200}

                    # Записываем ID респа в базу
                    write_particip_id(chat_id, username)

                    # Обновляем статус: ожидаем согласие
                    update_particip(username, {'status': "agreement waiting"})

                    # Отправлеям пользователськое соглашение
                    send_file(chat_id, AGREEMENT_TITLE, study_info['agreement_file'].decode('utf-8'))
                    
                    # Отправлеям клавиатуру с согласием
                    send_message_with_k(
                        chat_id, 
                        AGREEMENT_PROMPT, 
                        agreement_keyboard())

                # Участие в исследовании
                elif callback_data[1] == "agree":

                    # Проверка работоспособности клавиатуры
                    if not (particip_status == "agreement waiting" or particip_status == "disagreeing"):
                        send_message(chat_id, NON_ACTIVE_KEYBOARD)
                        return {'statusCode': 200}

                    # Удаляем предыдщие записи
                    delete_beeps(chat_id)

                    # Проверяем наличие часовой зоны
                    particip_settings = get_particip_settings(chat_id)
                    
                    if not particip_settings:
                        send_message(chat_id, TZ_REQUEST)
                        update_particip(username, {'status': "timezone_waiting"})
                        return {'statusCode': 200}

                    # Получаем часовую зону
                    timezone = particip_settings['timezone']

                    # Извлекаем базовые опросы
                    base_surveys = study_info['base_surveys'].decode('utf-8').split(', ')

                    # Проверяем наличие входного опросника
                    enterance_survey = study_info['enterance_survey']
                    if enterance_survey:
                        
                        # Убираем битовую кодировку входного опросника
                        enterance_survey = enterance_survey.decode('utf-8')

                        # Обновляем статус: ожидание входного опроса 
                        update_particip(username, {'status': "waiting_enterance_survey"})

                        # Формируем раписание
                        send_message(chat_id, ENTERANCE_SURVEY_MESSAGE)
                        form_schedule_es(chat_id, study_info, particip_info, enterance_survey, "enterance")

                    else:

                        # Обновляем статус: участие
                        update_particip(username, {'status': "participating"})

                        # Формируем раписание
                        form_schedule(chat_id, study_info, particip_info, timezone, base_surveys)
                    
                    # Завершаем обработку    
                    return {'statusCode': 200}

                # Несогласие с соглашением, исключение из исследования
                elif callback_data[1] == "disagree":
                
                    # Проверка работоспособности клавиатуры
                    if particip_status != "agreement waiting":
                        send_message(chat_id, NON_ACTIVE_KEYBOARD)
                        return {'statusCode': 200}

                    # Обновляем статус: ожидаем подтверждения
                    update_particip(username, {'status': "disagreeing", 'previous_status': particip_status})

                    # Отправлеям клавиатуру с проверкой несогласия
                    message_id = send_message_with_k(
                        chat_id, 
                        DISAGREE_CHECK, 
                        disagree_keyboard())
                    
                    # Сохраняем последнее сообщение
                    update_particip_settings(chat_id, ["focal_message_id"], [message_id])
                    return {'statusCode': 200}

            elif callback_data[0] == "disagree":

                # Проверка работоспособности клавиатуры
                if particip_status != "disagreeing":
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

                # Действительне ли решил не соглашаться
                if callback_data[1] == "confirm":
                    send_message(chat_id, BYE_BYE_MESSAGE)
                    delete_particip(username)
                
                # Ошибся
                elif callback_data[1] == "cancel":
                    print("was error")
                    message_id = get_particip_settings(chat_id)['focal_message_id']
                    print(message_id)
                    unsend_message(chat_id, message_id)
                    update_particip(username, {'status': previous_status})
                
                return {'statusCode': 200}

            elif callback_data[0] == "leave":

                # Проверка работоспособности клавиатуры
                if particip_status != "leaving_study":
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

                # Выход из исследования
                elif callback_data[1] == "confirm":
                    send_message(chat_id, BYE_BYE_MESSAGE)
                    delete_particip(username)
                    
                # Отмена выхода из исследования
                elif callback_data[1] == "cancel":
                    send_message(chat_id, LEAVE_CANCEL)
                    update_particip(username, {'status': previous_status})
                
                return {'statusCode': 200}

            elif callback_data[0] == "delete":

                # Проверка работоспособности клавиатуры
                print(1)
                print(particip_status)
                if particip_status not in ("deleting_all_data", "not_invited"):
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)
                    print(2)

                # Удаление данных
                elif callback_data[1] == "confirm":
                    send_message(chat_id, ALL_DATA_DELETED)
                    print(3)

                    # Проверяем, что нужно удалить
                    # Удаление и участия в исследовании, и настроек
                    if len(callback_data) == 4:
                        delete_particip(username)
                        delete_particip_settings(chat_id)
                   
                    else:
                        print(f"callback_data: {callback_data}")
                        entity_to_delete = callback_data[2]

                        # Удаление только участия в исследовании
                        if entity_to_delete == "study":
                            delete_particip(username)

                        # Удаление только настроек
                        elif entity_to_delete == "settings":
                            delete_particip_settings(chat_id)

                # Отмена выхода из исследования
                elif callback_data[1] == "cancel":
                    send_message(chat_id, DELETE_CANCEL)
                    update_particip(username, {'status': previous_status})
                
                return {'statusCode': 200}

            else:
                
                # Работаем по обновлению бипа
                response = callback_data[1]

                # Получаем номер бипа
                beep_id = callback_data[0]
                print(f"beep_id: {beep_id}")
                
                # Получаем информацию о бипе, который ожидает ответа
                question_id, message_id, question_type = get_beep_data(beep_id)

                # Информируем о начале работы
                edit_question_message(chat_id, message_id, LOADING_TEXT)

                # Проверяем не является ли бип, ожидающий ответа, запросом локации
                if question_type == 'location':
                    old_beep_loc = True
                else:
                    old_beep_loc = False

                # Обновляем сообщение – подставляем новый бип
                update_message(chat_id, message_id, question_id, old_beep_loc)

                # Обновляем данные о бипе в таблице
                update_beep_db(beep_id, response, message_id)

            return {'statusCode': 200}

    
        # Обработка сообщений
        elif 'message' in message:

            # Извлекаем chat_id 
            chat_id = message['message']['chat']['id']

            # Извлекаем username участника
            if 'username' in message['message']['chat']:
                username = message['message']['chat']['username']
            else:
                username = 'NoUsername'

            # Извлекаем текст сообщения
            if 'text' in message['message']:
                message_text = message['message']['text']
            else:
                message_text = '[no text]'

            # По ID отправителя получаем информацию о статусе исследования
            particip_info = get_particip_info(username)
            if not particip_info:
                particip_status = "not_invited"
            else:
                particip_status = particip_info["status"].decode('utf-8')
                previous_status = particip_info.get('previous_status')
                if previous_status is not None:
                    previous_status = previous_status.decode('utf-8')
                    
                study_id = particip_info["study_id"].decode('utf-8')
                study_info = get_study_info(study_id)
            print('Particip status: ', particip_status)

            ## ОБРАБОТКА КОМАНД
            # /start
            if '/start' in message_text:
                send_message(chat_id, START_MESSAGE)
                return {'statusCode': 200}

            # help
            elif '/help' in message_text:
                send_message(chat_id, HELP_MESSAGE)
                return {'statusCode': 200}

            # set_tz
            elif '/set_tz' in message_text:

                # Смотрим, есть ли часовая зона сейчас
                particip_settings = get_particip_settings(chat_id)
                if particip_settings:
                    message = TZ_EDITING.format(particip_settings['timezone'])
                else:
                    message = TZ_SETTING

                # Обновляем статус участника
                update_particip(username, {'status': 'setting_tz', 'previous_status': particip_status})

                send_message(chat_id, message)
                return {'statusCode': 200}
            
            # /leave_study
            elif '/leave_study' in message_text:
                if particip_status == "not_invited":
                    send_message(chat_id, NOWHERE_TO_LEAVE_MESSAGE)

                else:
                    update_particip(username, {'status': 'leaving_study', 'previous_status': particip_status})
                    send_message_with_k(chat_id, LEAVE_CONFIRM, study_leave_keyabord())

                return {'statusCode': 200}

            # /full_delete
            elif '/full_delete' in message_text:

                # Получаем наличие активных исследований
                partcipation_prescence = check_particip_studies(username)

                # Получаем наличие настроек
                particip_settings = check_particip_settings(chat_id)

                # Если нет ни настроек, ни исследований
                if not partcipation_prescence and not particip_settings:
                    send_message(chat_id, NO_DATA_TO_DELETE)
                    return {'statusCode': 200}
                
                # Если данные для удаления есть
                removal_list = ""
                removel_code = ""

                if partcipation_prescence:
                    removal_list += 'участие в исследовании'
                    removel_code += '_study'

                if particip_settings:
                    removel_code += '_settings'
                    if removal_list:
                        removal_list += ', '
                    removal_list += 'настройки'

                send_message_with_k(chat_id, ALL_DELETE_CONFIRM.format(removal_list), data_delete_keyabord(removel_code))
                update_particip(username, {'status': 'deleting_all_data', 'previous_status': particip_status})
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
                        STUDY_INFO_MESSAGE.format(study_info['name'].decode('utf-8'), study_info['description'].decode('utf-8')), 
                        join_keyboard())

                elif particip_status == "participating" or  particip_status == "agreement waiting":
                    send_message(chat_id, ALREADY_HERE_MESSAGE)

            if particip_status in ("answers_expected", "waiting_enterance_survey"):

                # Получаем информацию о бипе, который ожидает ответа
                beep_id, question_id, message_id, question_type = get_beep_to_write(chat_id)

                # Проверяем не является ли бип, ожидающий ответа, запросом локации
                if question_type == 'location':
                    old_beep_loc = True
                else:
                    old_beep_loc = False

                # Проверяем сообщение
                correct, upd_input = check_text_input(message_text)

                # Если отправлен текст – все ок, идем дальше
                if correct and question_type == 'open':

                    # Обновляем сообщение – подставляем новый бип
                    update_message(chat_id, message_id, question_id, old_beep_loc)

                    # Обновляем данные о бипе в таблице
                    update_beep_db(beep_id, upd_input, message_id)

                # Если не ок – отправляем сообщение об этом
                elif question_type != 'open':
                    send_message(chat_id, TEXT_NOT_VALID)
                    
                else: 
                    send_message(chat_id, INCORRECT_INPUT)
            
            elif particip_status == "timezone_waiting":
                
                # Проверяем ввод
                correct, message_text, timezone = hour_validator(message_text)
                if not correct:
                    send_message(chat_id, INCORRECT_HOUR)

                else:

                    # Записываем информацию о часовом поясе участника
                    write_particip_settings("particip_id, timezone, tz_changes", f"'{chat_id}', {timezone}, 0")
                
                    # Проверяем наличие входного опросника
                    enterance_survey = study_info['enterance_survey']
                    if enterance_survey:

                        # Убираем битовую кодировку входного опросника
                        enterance_survey = enterance_survey.decode('utf-8')

                        # Обновляем статус: ожидание входного опроса 
                        update_particip(username, {'status': "waiting_enterance_survey"})

                        # Формируем раписание
                        send_message(chat_id, ENTERANCE_SURVEY_MESSAGE)
                        form_schedule_es(chat_id, study_info, particip_info, enterance_survey, "enterance")

                    else:

                        # Обновляем статус: участие
                        update_particip(username, {'status': "participating"})

                        # Формируем раписание
                        form_schedule(chat_id, study_info, particip_info, timezone, base_surveys)

                    # Завершаем обработку    
                    return {'statusCode': 200} 

            # Изменение (настройка) часового пояса
            elif particip_status == "setting_tz": 

                # Проверяем ввод
                correct, message_text, timezone = hour_validator(message_text)
                if not correct:
                    send_message(chat_id, INCORRECT_HOUR)

                else:
                    # Смотрим, есть ли часовой пояc у участника 
                    particip_settings = get_particip_settings(chat_id)
                    if particip_settings['timezone']:
                        old_tz = particip_settings['timezone']
                        change_tz = timezone - old_tz

                    # Записываем часовой пояс в базу
                    update_particip_settings(chat_id, ["timezone"], [timezone])

                    # Меняем время бипов
                    if particip_settings['timezone']:
                        print(f"change_tz: {change_tz}")
                        change_beeps_tz(chat_id, change_tz)
                    else:
                        print("NAAAAH")

                    # Информируем о настройке часового пояса
                    send_message(chat_id, TZ_SETTED.format(timezone))

                    # Увеличиваем кол-во изменений часового пояса
                    tz_changes_increase(chat_id)

                    # Возвращаем статус
                    update_particip(username, {'status': previous_status})

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
                    update_particip_by_id(row['participant_id'].decode('utf-8'), {'status': "answers_expected"})
                    
        
        # Работаем с истекшими бипами
        users_to_check = check_expired_beeps()
        if users_to_check:
            for user_id in users_to_check:

                # Проверяем закончилось ли исследование
                if check_study_end(user_id):

                    # Смотрим есть ли выходной опросник
                    # Получаем информацию участника
                    particip_info = get_particip_info(user_id, True)

                    # Получаем информацию об исследовании
                    study_info = get_study_info(particip_info['study_id'].decode('utf-8'))

                    # Если выходной опросник есть, то отправляем его
                    exit_survey = study_info['exit_survey']
                    if exit_survey:

                        # Уведомляем участника о выходном опросе
                        send_message(user_id, EXIT_SURVEY_MESSAGE)

                        # Убираем битовую кодировку входного опросника
                        exit_survey = exit_survey.decode('utf-8')

                        # Обновляем статус: ожидание входного опроса 
                        update_particip_by_id(user_id, {'status': "waiting_exit_survey"})

                        # Формируем раписание
                        form_schedule_es(user_id, study_info, particip_info, exit_survey, "exit")
                        
                    # Если выходного опросника нет, то завершаем исследование
                    else:
                        # Обновляем статус: завершил участия
                        update_particip_by_id(user_id,  {'status': "ended"})

                        # Если закончилось, то завершаем его
                        time.sleep(10)
                        send_message(user_id, THE_END_MESSAGE)

                # Если исследование не закончилось, то просто говорим системе о том, что опрос окончился
                else:
                    update_particip_by_id(user_id,  {'status': "participating"})

        # Проверяем прохождение входных опросов
        particips_to_set = check_enterance_beeps()
        if particips_to_set:
            for particip_id in particips_to_set:

                # Обновляем статус на участие
                update_particip_by_id(particip_id, {'status': "participating"})

                # Получаем информацию участника
                particip_info = get_particip_info(particip_id, True)

                # Получаем информацию об исследовании
                study_info = get_study_info(particip_info['study_id'].decode('utf-8'))

                # Получаем часовую зону
                particip_settings = get_particip_settings(particip_id)
                timezone = particip_settings['timezone']

                # Извлекаем базовые опросы
                base_surveys = study_info['base_surveys'].decode('utf-8').split(', ')

                # Формируем раписание
                form_schedule(particip_id, study_info, particip_info, timezone, base_surveys)

        # Проверяем прохождение выходного опросника
        particips_to_end = check_exit_beeps()
        if particips_to_end:
            for particip_id in particips_to_end:

                # Если исследование ещё не завершено, то завршаем его
                particip_info = get_particip_info(particip_id, True)
                status = particip_info.get('status')
                if status:
                    status = status.decode('utf-8')
                else:
                    return {'statusCode': 200}

                if status == "waiting_exit_survey":

                    # Обновляем статус: завершил участия
                    update_particip_by_id(particip_id,  {'status': "ended"})

                    # Если закончилось, то завершаем его
                    time.sleep(10)
                    send_message(particip_id, THE_END_MESSAGE)
                
                else:
                    return {'statusCode': 200}


    return {
        'statusCode': 200,
        'body': 'Everityhing is cool: beeps are beeping, schedules are scheduling, and users are using!'
    }
    
