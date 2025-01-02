# LIBRARIES
import json
import requests
from datetime import datetime, timedelta
import time

# OTHER SCRIPTS
from config import *
from sql_requests import *


# БАЗОВЫЕ ФУНКЦИИ
# 0. Функция для отправки сообщения c текстом (UPD)
def send_message(chat_id, text):
    url = URL + f"sendMessage?parse_mode=markdown&chat_id={chat_id}&text={text}"
    print(url)
    requests.get(url)


# 1. Функция для отправки сообщения c текстом и клавиатурой (UPD)
def send_message_with_k(chat_id, text, keyboard):
    url = URL + f"sendMessage?text={text}&chat_id={chat_id}&parse_mode=markdown&reply_markup={keyboard}"  
    requests.get(url)


# 2. Функция для отправки документа (UPD)
def send_file(chat_id, text, file_url):

    url = URL + "sendDocument"
    data = {
        'document': file_url,
        'chat_id': chat_id,
        'comment': text,
        'parse_mode': 'Markdown'
    }
    requests.post(url, data=data)


# 3. Функция для отправки сообщения c текстом (опрос) (UPD)
def send_question_message(chat_id, text, keyboard=None):
    if not keyboard:
        url = URL + f"sendMessage?text={text}&chat_id={chat_id.decode('utf-8')}&parse_mode=markdown"
    else:
        url = URL + f"sendMessage?text={text}&chat_id={chat_id.decode('utf-8')}&reply_markup={keyboard}&parse_mode=markdown"
    res = requests.get(url)

    message_id = res.json()["result"]["message_id"]
    user_id = res.json()["result"]["chat"]["id"]
    update_beep_data(message_id, user_id)


# 4. Функция для редактирования сообщений (опрос) (UPD)
def edit_question_message(chat_id, message_id, text, keyboard=None):
    if not keyboard:
        url = URL + f"editMessageText?text={text}&chat_id={chat_id}&message_id={message_id}&parse_mode=markdown"
    else:
        url = URL + f"editMessageText?text={text}&chat_id={chat_id}&reply_markup={keyboard}&message_id={message_id}&parse_mode=markdown"
    requests.get(url)


# 5. Функция для создания клавитауры вопроса (UPD)
def create_keyboard(question_df, participant_id, beep_id, question_id, sent_time):

    # Загатовка для клавиатуры
    raw_keyboard = []

    # Подготавливаем варианты ответов
    response_options = []
    response_keys = [key for key in question_df.keys() if 'response_' in key]
    sorted_keys = sorted(response_keys, key=lambda x: (int(x.split('_')[1]), x))

    for col in sorted_keys:
        if 'response_' in col and question_df[col].decode('utf-8') != "nan":
            print(question_df[col])
            response_options.append(question_df[col])

    # Определяем количество строк
    rows_amount = question_df['rows_amount']
    buttons_per_row = len(response_options) // rows_amount

    # Проходимся по каждой строке
    for i in range(rows_amount):
        row = []
        for j in range(buttons_per_row):
            index = i * buttons_per_row + j
            if index < len(response_options):
                row.append({
                    "text": response_options[index].decode('utf-8'),
                    "callback_data": f"{beep_id}_{response_options[index].decode('utf-8')}_{question_id}"
                })

        # Добавляем строку в клавиатуру
        raw_keyboard.append(row)

    # Возвращаем клавиатуру
    keyboard =  json.dumps({"inline_keyboard": raw_keyboard})
    
    return keyboard


# 6. Функция для формирования вопроса (UPD)
def create_quest_text(question_id, question, comment, expire_time):

    # Форматируем текст сообщения
    formatted_time = datetime.fromtimestamp(expire_time).strftime('%H:%M')
    # Формируем текст сообщения
    if comment != "nan":
        question_text = QUESTION_TEXT_CAPTION.format(question_id, formatted_time, question, comment)
    else:
        question_text = QUESTION_TEXT.format(question_id, formatted_time, question)

    return question_text


# 7. Функция для подготовки бипов (UPD)
def prepare_beep(beep_data):

    # Получаем информаии о вопросе для отправки
    question_df = get_survey_quest(beep_data['study_id'], beep_data['question_id'])
    # Получаем информацию о типе вопроса
    question_type = question_df['question_type']

    # Формируем текст для отправки
    question_text = create_quest_text(
        question_df['question_num'],
        question_df['question'].decode('utf-8'),
        question_df['comment'].decode('utf-8'),
        beep_data['expire_time'])

    # Формируем клавиатуру для отправки
    if question_type == 'open':
        keyboard = None
    else:
        keyboard = create_keyboard(
            question_df, 
            beep_data['participant_id'], 
            beep_data['beep_id'], 
            beep_data['question_id'], 
            beep_data['time_to_send'])

    # Возвращаем id участника (куда отправляем), текст и клавиатуру опросника (что отправляем)
    return beep_data['participant_id'], question_text, keyboard


# 8. Функция для отправки бипов новым сообщением (UPD)
def send_beep(participant_id, question_text, keyboard):
    send_question_message(participant_id, question_text, keyboard)


# 9.1. Функция заглушка для отображения прогресса
def display_loading(user_id, message_id, stop_event):
    loading_texts = ["_Загрузка_", "_Загрузка._", "_Загрузка.._", "_Загрузка..._"]
    i = 0
    while not stop_event.is_set():
        question_text = loading_texts[i % len(loading_texts)]
        edit_question_message(user_id, message_id, question_text)
        i += 1
        time.sleep(0.1)


# 9.2. Функция для отправки бипов в том жем сообщении (UPD)        
def update_message(user_id, message_id, question_id):
    next_beep = get_next_beep(user_id, message_id, question_id)

    if not next_beep:
        question_text = SURVEY_COMPLETE_MESSAGE
        edit_question_message(user_id, message_id, question_text)  
    else:
        try:
            question_df = get_survey_quest(next_beep['study_id'], next_beep['question_id'])
            question_text = create_quest_text(
                question_df['question_num'],
                question_df['question'].decode('utf-8'),
                question_df['comment'].decode('utf-8'),
                next_beep['expire_time'])

            if question_df['question_type'] != 'open':
                keyboard = create_keyboard(
                    question_df, next_beep['participant_id'], next_beep['beep_id'], next_beep['question_id'], next_beep['time_to_send'])
            else:
                keyboard = None

            # Отправка нового бипа
            edit_question_message(user_id, message_id, question_text, keyboard)

        except Exception as e:
            print(e)


# РАССЫЛКИ
# 11. Функция для формирования бипов для отправки (UPD)
def form_beep_dicts(chat_id, study_id, surveys_len, days, time_to_send, expire_time):
    
    beeps_list = []
    for day in range (0, days + 1):
        for beep in range(int(surveys_len)):
            beep_dict = {}
            beep_dict['participant_id'] = chat_id
            beep_dict['study_id'] = study_id
            beep_dict['question_id'] = beep + 1
            beep_dict['time_to_send'] = (time_to_send + timedelta(days=day)).strftime('%Y-%m-%dT%H:%M:%SZ')
            beep_dict['expire_time'] = (time_to_send + timedelta(minutes=expire_time, days=day)).strftime('%Y-%m-%dT%H:%M:%SZ')
            beep_dict['question_type'] = get_question_type(beep_dict['study_id'], beep + 1).decode('utf-8')
            beeps_list.append(beep_dict)

    return beeps_list


# 12. Функция для проверки и закрытия истекших бипов (UPD)
def check_expired_beeps():

    # Получаем пользователей для дальнейших действий
    users_to_check, dicts_to_edit = handle_expired_beeps()

    # Закрываем истекшие опросники опросники
    if dicts_to_edit:
        for dictinoary in dicts_to_edit:
            edit_question_message(
                dictinoary['chat_id'], 
                dictinoary['message_id'], 
                SURVEY_EXPIRE_MESSAGE)

    # Возвращаем опросники для дальнейшей проверки
    return users_to_check
