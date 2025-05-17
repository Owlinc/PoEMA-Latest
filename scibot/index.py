# LIBRARIES
import requests
import re
import os
import json
import pandas as pd

# OTHER SCRIPTS
from sql_requests import *
from utils import *
from validators import *

# HANDLER
def handler(event, context):

    try:
        message = json.loads(event['body'])
    except Exception as error:
        message = False

    if  message:
        if 'callback_query' in message:

            # Парсим колбэк
            callback = message['callback_query']['data']
            callback_split = callback.split('_')

            chat_id = message['callback_query']['message']['chat']['id']
            study_info = get_study_info(chat_id)
            if not study_info:
                study_status = "not_initiated"
            else:
                study_status = study_info['status'].decode('utf-8')
                previous_status = study_info.get('previous_status')
                if previous_status is not None:
                    previous_status = previous_status.decode('utf-8')

            if callback == "study_launch":

                if study_status != 'launched':

                    # Статус 10: исследование запущено
                    update_study(chat_id, {'status': 'launched'})
                    send_message(chat_id, STUDY_WAS_LAUNCHED)

                    # Добавление участников в базу
                    participants_usernames = study_info['participants_usernames'].decode('utf-8').split(', ')
                    for participant_username in participants_usernames:
                        initaite_particip(participant_username, chat_id)
                
                else:
                    send_message(chat_id, STUDY_ALREADY_LAUNCHED)
            
            elif callback == "clean_confirm":               
            
                if study_status == "study_cleaning":
                    send_message(chat_id, SUCCESS_CLEAN)
                    update_study(chat_id, {'status': previous_status, 'previous_status': 'study_cleaning'})
                    
                    delete_study(chat_id)
                    delete_survey_sql(chat_id)
                    delete_participation(chat_id)
                    delete_beeps(chat_id)
                    delete_file(study_info['agreement_file'].decode('utf-8'))

                else:
                    print(1)
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)
            
            elif callback == "clean_cancel":

                if study_status == "study_cleaning":
                    update_study(chat_id, {'status': previous_status, 'previous_status': 'study_cleaning'})
                    send_message(chat_id, CLEAN_CANCEL)

                else:
                    print(2)
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

            # Нужен ли входной опросник
            elif callback_split[0] == "entersurvdec":

                if study_status == "several_surveys_added":

                    if callback_split[1] == "confirm":

                        # Отправляем клавиатуру с опросниками
                        keyboard = survey_choice_keyboard(chat_id)
                        send_message_with_k(chat_id, ENTER_SURVEY_CHOICE, keyboard)
                        update_study(chat_id, {'status': "enter_survey_waiting", 'previous_status': study_status})

                    elif callback_split[1] == "reject":
                        
                        # Отправляем клавиатуру для принятия решения о том, понадобится ли выходной опросник
                        send_message_with_k(chat_id, EXIT_SURVEY_DEC, exit_survey_dec_keyboard())
                        update_study(chat_id, {'status': "exit_survey_decision", 'previous_status': study_status})

                else:
                    print(3)
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

            # Нужен ли выходной опросник
            elif callback_split[0] == "exitsurvdec":

                if study_status == "exit_survey_decision":

                    if callback_split[1] == "confirm":

                        update_study(chat_id, {'status': "exit_survey_waiting", 'previous_status': study_status})
                        
                        # Формируем клавиатуру с опросниками
                        enter_survey = study_info.get("enterance_survey")
                        if enter_survey:
                            enter_survey = enter_survey.decode('utf-8')
                        
                        keyboard = survey_choice_keyboard(chat_id, True, enter_survey)
                        
                        # Отправляем клавиатуру с опросниками
                        send_message_with_k(chat_id, EXIT_SURVEY_CHOICE, keyboard)

                    elif callback_split[1] == "reject":

                        # Сохранение базовых опросов
                        base_surveys, base_surveys_str = get_base_surveys(chat_id, study_info)
                        update_study(chat_id, {
                            'base_surveys': base_surveys_str, 
                            'status': "survey_added", 
                            'previous_status': study_status})

                        # Запрос времени для прохождения опросов
                        send_message(chat_id, SURVEY_SETTING_END.format(base_surveys[0]))

                else:
                    print(4)
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

            # Сохранение входного опросника
            elif callback_split[0] == "entersurv":

                if study_status == "enter_survey_waiting":

                    # Определние выбранного опросника
                    enter_survey = callback_split[1]

                    # Сохранение выбранного опросника в базу
                    update_study(chat_id, {'enterance_survey': enter_survey})

                    # Проверяем, остались ли у нас опросники для выходного исследования
                    study_info = get_study_info(chat_id)
                    base_surveys, base_surveys_str = get_base_surveys(chat_id, study_info)
                    bs_amount = len(base_surveys)

                    # Если остались
                    if bs_amount > 1:

                        # Отправляем клавиатуру для принятия решения о том, понадобится ли выходной опросник
                        send_message_with_k(chat_id, EXIT_SURVEY_DEC, exit_survey_dec_keyboard())
                        update_study(chat_id, {'status': "exit_survey_decision", 'previous_status': study_status})

                    # Если не осталось
                    else:
                        # Запрос времени для прохождения опросов
                        send_message(chat_id, SURVEY_SETTING_END.format(base_surveys[0]))

                        # Сохранение базовых опросов
                        update_study(chat_id, {
                            'base_surveys': base_surveys_str, 
                            'status': "survey_added", 
                            'previous_status': study_status})
                
                else:
                    print(5)
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

            # Сохранение выходного опросника
            elif callback_split[0] == "exitsurv":

                if study_status == "exit_survey_waiting":

                    # Определние выбранного опросника
                    exit_survey = callback_split[1]

                    # Сохранение выбранного опросника в базу
                    update_study(chat_id, {'exit_survey': exit_survey})
                    study_info['exit_survey'] = exit_survey

                    # Извлечение базовых опросов
                    base_surveys, base_surveys_str = get_base_surveys(chat_id, study_info)

                    # Запрос времени для прохождения опросов
                    send_message(chat_id, SURVEY_SETTING_END.format(base_surveys[0]))

                    # Сохранение базовых опросов
                    update_study(chat_id, {
                        'base_surveys': base_surveys_str, 
                        'status': "survey_added", 
                        'previous_status': study_status})

                else:
                    print(6)
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

            # Решение об одном времени для всех опросников
            elif callback_split[0] == "onetldec":

                if study_status == "one_tl_decision":

                    # Получаем базовые опросники
                    base_surveys = study_info["base_surveys"].decode('utf-8').split(', ')

                    # Получаем текущее время прохождения
                    current_tl = study_info["completion_tl"].decode('utf-8').split('; ')

                    # Извлекаем время прохождения и следующий опросник 
                    tl, next_survey = handle_tl_for_all(base_surveys, current_tl)

                    if callback_split[1] == "confirm":

                        # Отправляем сообщение об успехе + запрашиваем соглашение
                        message_text = CORRECT_TL_ALL.format(tl)
                        send_message(chat_id, message_text)

                        # Настраиваем одно время прохождения для всех опросов
                        tl_surveys = form_one_tl_surveys(base_surveys, tl)
                        print(tl_surveys)

                        # Меняем статус исследования на соответсвующий
                        update_study(chat_id, {'status': 'comp_tl_added', 'completion_tl': tl_surveys})
                    
                    elif callback_split[1] == "reject":

                        # Запрашиваем время прохождения для следующего опроса
                        # Отправляем сообщение об успехе + запрашиваем соглашение
                        message_text = DUR_REQUEST_TEXT_AO.format(next_survey)
                        keyboard = tl_choice_keyboard([tl])
                        send_message_with_k(chat_id, message_text, keyboard)

                        # Меняем статус исследования на соответсвующий
                        update_study(chat_id, {'status': 'survey_added'})

                else:
                    print(7)
                    send_message(chat_id, NON_ACTIVE_KEYBOARD)

            # Запись времени прохождения для опросника
            elif callback_split[0] == "tloption":

                # Извлечение времени
                time_limit = callback_split[1]

                # Обработка ввода 
                general_handle_tl(chat_id, study_info, time_limit)


        ## ОБРАБОТКА СООБЩЕНИЙ
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
            if not study_info:
                study_status = "not_initiated"
            else:
                study_status = study_info["status"].decode('utf-8')
                print('Study status: ', study_status)

            # Обрабатываем стаднартные команды
            # /start
            if "/start" in message_text:
                send_message(chat_id, START_MESSAGE)
                return {'statusCode': 200}

            elif "/help" in message_text:
                send_message(chat_id, FULL_HELP_MESSAGE)
                return {'statusCode': 200}

            # /clean_study
            elif "/clean_study" in message_text:
                if study_status == "not_initiated":
                    send_message(chat_id, NOTHING_TO_CLEAN)
                    return {'statusCode': 200}
                else:
                    update_study(chat_id, {'status': 'study_cleaning', 'previous_status': study_status})
                    send_message_with_k(chat_id, CLEAN_CONFIRM, clean_confirm_keyboard())
                    return {'statusCode': 200}

            # /get_data
            elif "/get_data" in message_text:
            
                if study_status != "launched":
                    send_message(chat_id, DATA_ERROR_NOT_LAUNCHED)
                    return {'statusCode': 200}
                # Проверяем наличие данных, если их нет говорим об этом
                # Если есть отправляем сообщение с сслыкой на выгрузку
                data_prescence, file_url, text_message = get_beeps_data(chat_id)
                send_message(chat_id, text_message)
                return {'statusCode': 200}

            # /set_start_date
            elif "/set_start_date" in message_text:
            
                if study_status != "launched":
                    send_message(chat_id, SDATE_ERROR_NOT_LAUNCHED)
                    return {'statusCode': 200}

                elif message_text.strip() == '/set_start_date':
                    # Информируем о том, как указаывать стартовую дату
                    send_message(chat_id, SET_START_DATE_INFO)
                    return {'statusCode': 200}

                # Проверяем введенную дату
                correct, start_date, answer = date_validator(message_text)
                if correct:
                    update_study(chat_id, {'start_date': start_date})

                # Информируем о результате ввода
                send_message(chat_id, answer)
                return {'statusCode': 200}

            # /analyse_opens
            elif "/analyse_opens" in message_text:

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
                else:
                    print("SMTH WRONG")

                # Информируем об успешности этапа
                send_message(chat_id, message_text)

            # Статус 3: описание добавлено, ожидается опросник
            elif study_status == "described":

                # Проверяем наличие подходящего файла
                if 'document' in message['message']:

                    # Проверяем корректнотсь расширения
                    if message['message']['document']['file_name'].endswith(".xlsx"):
                        file_id = message['message']['document']['file_id']
                        file_url = get_file_url(file_id)
                        survey_name = SURVEY_STRUCT_FILE_NAME.format(chat_id)
                        survey_file_url = upload_file_to_storage(file_url, chat_id, survey_name)
                        
                        # Проверяем корректнотсь заполнения опросника
                        correct, survey_df, response, keyboard, surveys_name, surveys_amount = all_surveys_validator(survey_file_url)

                        if correct:
                            
                            # Меняем стату в зависимости от количества опросов
                            if surveys_amount == 1:
                                send_message(chat_id, response)

                                # Загружаем опросник(и) в базу
                                upload_surveys_sql(survey_df, chat_id)
                                base_surveys, base_surveys_str = get_base_surveys(chat_id, study_info)
                                update_study(chat_id, {'status': 'survey_added', 'base_surveys': base_surveys_str})
                            else:
                                # Загружаем опросник(и) в базу
                                upload_surveys_sql(survey_df, chat_id)
                                send_message_with_k(chat_id, response, keyboard)
                                update_study(chat_id, {'status': 'several_surveys_added'})

                        else:
                            send_message(chat_id, SURVEY_ERROR.format(response))
                    else:
                        send_message(chat_id, WRONG_SURVEY_FILE)
                else:
                    send_message(chat_id, NO_FILE_SURVEY)

            # Статус 4: опросник добавлен, ожидается ограничение по времени прохождения опроса +
            elif study_status == "survey_added":

                general_handle_tl(chat_id, study_info, message_text)

            # Статус 5: ограничение по времени прохождения опроса добавлено, ожидается пользовательское соглашение +
            elif study_status == "comp_tl_added":

                # Проверяем наличие подходящего файла
                if 'document' in message['message']:
                    if message['message']['document']['file_name'].endswith(".pdf"):
                        file_id = message['message']['document']['file_id']
                        file_url = get_file_url(file_id)
                        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=3))
                        comb_name = f"{chat_id}_{random_suffix}"
                        aggr_file_name = f"{AGGR_FILE_NAME.format(comb_name)}"

                        print(f"aggr_name: {aggr_file_name}")
                        agreement_file = upload_file_to_storage(file_url, chat_id, aggr_file_name)
                        print(f"agreement_file: {agreement_file}")
                        
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
                    send_photo_message(chat_id, message_text, USERNAME_GUIDE)
                
                else:
                    send_message(chat_id, message_text)

            # Статус 7: длительность исследования указана, ожидаются юзернеймы участников +
            elif study_status == "duration_added":

                # Если все ок с участниками, добавляем их
                correct, response, participants = participants_validator(message_text)
                if correct:

                    update_study(chat_id, {'participants_usernames': participants})

                    # Получаем базовые опросники
                    base_surveys = study_info["base_surveys"].decode('utf-8').split(',')
                    base_surveys_amount = len(base_surveys)

                    # Смотрим на кол-во опросников
                    if base_surveys_amount == 1:
                        status = 'participants_added'
                    else:
                        status = 'several_pt_expected'

                    # Обновляем статус участия  
                    update_study(chat_id, {'status': status})

                    # Добавляем информацию об опроснике
                    response = response.format(base_surveys[0])

                # Информируем об успешности этапа
                send_message(chat_id, response)
                return {'statusCode': 200} 
        
            # Статус 8.1: участники добавлены, ожидаются временные точки для нескольких рассылок +
            elif study_status in ("several_pt_expected", "last_tp_expected"):

                # Получаем текущие временне точки и интервалы
                surveys_pt = study_info.get("prompting_time")
                if surveys_pt:
                    surveys_pt = surveys_pt.decode('utf-8').split('; ')

                # Получаем базовые опросники
                base_surveys = study_info["base_surveys"].decode('utf-8').split(', ')
                base_surveys_amount = len(base_surveys)

                # Если все ок с временными точками, добавляем их в связке с опросом
                correct, response, prompting_time, setted_survey, next_survey, penult_survey = pt_validator_multi(message_text, surveys_pt, base_surveys)
                if correct:

                    # Записываем обнволенную информацию о времени для отправки опросов
                    update_study(chat_id, {'prompting_time': prompting_time})

                    # Если опрос непоследний, то меняем статус
                    if study_status != "last_tp_expected":

                        # Формируем сообщение о промежуточном прогрессе
                        response = TP_REQUEST_TEXT.format(setted_survey, next_survey)

                        # Если предпоследний опрос, то меняем на соответсвующий статус
                        if penult_survey:
                            update_study(chat_id, {'status': 'last_tp_expected'})

                    # Если опрос последний
                    else:
                        # Формируем сообщение о конце этапа
                        response = CORRECT_TP.format(setted_survey)

                    # Информируем об успешности этапа
                    send_message(chat_id, response)

                else:
                    # Информируем о неуспешности этапа
                    send_message(chat_id, response)
                    return {'statusCode': 200} 


            # Статус 8.2: участники добавлены, ожидаются временные точки для рассылки +
            elif study_status == "participants_added":

                # Получаем название базового опросника
                base_survey_name = study_info["base_surveys"].decode('utf-8').split(',')[-1]

                # Если все ок с временными точками, добавляем их
                correct, response, prompting_time = pt_validator(message_text)
                prompting_time_upd = f"{base_survey_name}={prompting_time}"

                if correct:
                    # Информируем об успешности этапа
                    send_message(chat_id, response.format(base_survey_name))
                    update_study(chat_id, {'status': 'pt_added', 'prompting_time': prompting_time_upd})
                else:
                    # Информируем о неуспешности этапа
                    send_message(chat_id, response)
                    return {'statusCode': 200} 


            # Статус 9: дата окончания подключения указана, ожидается запуск исследования
            if study_status in ("participants_added", "last_tp_expected"):

                # Информируем об исследовании и предлагаем запустить его
                study_info = get_study_info(chat_id)

                completion_tl = format_sev_surveys(study_info['completion_tl'].decode('utf-8'), "мин.")
                prompting_time = format_sev_surveys(study_info['prompting_time'].decode('utf-8'))
                base_surveys, base_surveys_str = get_base_surveys(chat_id, study_info)

                enterance_survey = study_info['enterance_survey']
                exit_survey = study_info['exit_survey']
                if enterance_survey:
                    enterance_survey = enterance_survey.decode('utf-8')
                else: 
                    enterance_survey = '–'
                if exit_survey:
                    exit_survey = exit_survey.decode('utf-8')
                else:
                    exit_survey = '–'

                send_message_with_k(
                    chat_id, 
                    LAUNCH_STUDY.format(
                        study_info['name'].decode('utf-8'), 
                        study_info['description'].decode('utf-8').replace("_", "\_"), 
                        study_info['participants_usernames'].decode('utf-8').replace("_", "\\_"),

                        enterance_survey,
                        base_surveys_str,
                        exit_survey,

                        completion_tl,
                        prompting_time,
                        study_info['duration']), 
                    launch_keyboard())

        else:
            send_message(chat_id, "Вы отправили что-то необычное. Я такое не понимаю....")
        
    return {
        'statusCode': 200,
        'body': 'Everityhing is cool: beeps are beeping, schedules are scheduling, and users are using!'
        }
