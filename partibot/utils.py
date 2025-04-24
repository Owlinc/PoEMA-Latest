# LIBRARIES
import json
import requests
from datetime import datetime, timedelta, date
import time
import random
import re

# OTHER SCRIPTS
from config import *
from sql_requests import *


# БАЗОВЫЕ ФУНКЦИИ
# 0.1. Функция для отправки сообщения c текстом
def send_message(chat_id, text):
    url = URL + f"sendMessage?parse_mode=markdown&chat_id={chat_id}&text={text}"
    
    start_time = time.time()
    while time.time() - start_time < 15:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get("result", {}).get("message_id")
            break
        else:
            print("Ошибка при отправке:", response.text)
            time.sleep(0.1)
    
    url = URL + f"sendMessage?parse_mode=markdown&chat_id={chat_id}&text={SENDING_ERROR_TEXT}"
    requests.get(url)
    print("ОШИБКА: Не удалось отправить сообщение после 15 секунд попыток")
    return None

# 0.2. Функция для удаления сообщения
def unsend_message(chat_id, message_id):
    url = URL + f"deleteMessage?chat_id={chat_id}&message_id={message_id}"
    requests.get(url)

# 0.3. Функция для отправки эффекта печати
def send_typing_effect(chat_id):
    url = URL + f"sendChatAction?chat_id={chat_id}&action=typing"
    requests.get(url)

# 1. Функция для отправки сообщения c текстом и клавиатурой
def send_message_with_k(chat_id, text, keyboard):
    url = URL + f"sendMessage?text={text}&chat_id={chat_id}&parse_mode=markdown&reply_markup={keyboard}"
    
    start_time = time.time()
    while time.time() - start_time < 15:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get("result", {}).get("message_id")
            break
        else:
            print("Ошибка при отправке:", response.text)
            time.sleep(0.1)
    
    # Отправка сообщения об ошибке
    error_url = URL + f"sendMessage?parse_mode=markdown&chat_id={chat_id}&text={SENDING_ERROR_TEXT}"
    requests.get(error_url)
    print("ОШИБКА: Не удалось отправить сообщение с клавиатурой после 15 секунд попыток")
    return None


# 2. Функция для отправки документа
def send_file(chat_id, text, file_url):

    url = URL + "sendDocument"
    data = {
        'document': file_url,
        'chat_id': chat_id,
        'comment': text,
        'parse_mode': 'Markdown'
    }
    requests.post(url, data=data)


# 3. Функция для отправки сообщения c текстом (опрос)
def send_question_message(chat_id, text, keyboard=None):

    if isinstance(chat_id, bytes):
        chat_id = chat_id.decode('utf-8')
    if not keyboard:
        url = URL + f"sendMessage?text={text}&chat_id={chat_id}&parse_mode=markdown"
    else:
        url = URL + f"sendMessage?text={text}&chat_id={chat_id}&reply_markup={keyboard}&parse_mode=markdown"

    start_time = time.time()
    while time.time() - start_time < 15:
        res = requests.get(url)
        if res.status_code == 200:
            print('updating beep_data via send_question_message')
            message_id = res.json()["result"]["message_id"]
            user_id = res.json()["result"]["chat"]["id"]
            update_beep_data(message_id, user_id)
            return message_id
            break
        else:
            print("Ошибка при отправке:", res.text)
            time.sleep(0.1)


# 4. Функция для редактирования сообщений
def edit_question_message(chat_id, message_id, text, keyboard=None):

    if not keyboard:
        url = URL + f"editMessageText?text={text}&chat_id={chat_id}&message_id={message_id}&parse_mode=markdown"
    else:
        url = URL + f"editMessageText?text={text}&chat_id={chat_id}&reply_markup={keyboard}&message_id={message_id}&parse_mode=markdown"

    start_time = time.time()
    while time.time() - start_time < 15:
        res = requests.get(url)
        if res.status_code == 200:
            return res.json().get("result", {}).get("message_id")
        else:
            print("Ошибка при отправке:", res.text)
            time.sleep(0.1)
            

# 5. Функция для создания клавитауры вопроса
def create_keyboard(question_df, participant_id, beep_id, question_id, sent_time, question_type):

    # Загатовка для клавиатуры
    raw_keyboard = []

    # Подготавливаем варианты ответов
    response_options = []
    response_keys = [key for key in question_df.keys() if 'response_' in key]
    sorted_keys = sorted(response_keys, key=lambda x: (int(x.split('_')[1]), x))

    for col in sorted_keys:
        if 'response_' in col and question_df[col].decode('utf-8') != "nan":
            response_options.append(question_df[col])

    # Создание базы для клавиатуры вопросов типа single choice и multiple choice
    if question_type in ("single_choice", "multiple_choice"):

        # Определяем количество строк
        rows_amount = question_df['rows_amount']
        buttons_per_row = len(response_options) // rows_amount

    # Создание клавиатуры для вопросов типа multiple choice
    if question_type == "single_choice":

        # Проходимся по каждой строке
        for i in range(rows_amount):
            row = []
            for j in range(buttons_per_row):
                index = i * buttons_per_row + j
                if index < len(response_options):
                    callback_text = sanitize_callback_data(response_options[index].decode('utf-8'))

                    row.append({
                        "text": response_options[index].decode('utf-8'),
                        "callback_data": f"{beep_id}_{callback_text}_{question_id}"
                    })

            # Добавляем строку в клавиатуру
            raw_keyboard.append(row)

        # Формируем клавиатуру
        keyboard = json.dumps({"inline_keyboard": raw_keyboard})

    # Создание клавиатуры для вопросов типа single choice
    elif question_type == "multiple_choice":

        # Проходимся по каждой строке
        for i in range(rows_amount):
            row = []
            for j in range(buttons_per_row):
                index = i * buttons_per_row + j
                if index < len(response_options):
                    callback_text = sanitize_callback_data(response_options[index].decode('utf-8').strip())
                    row.append({
                        "text": "⬜️ " + response_options[index].decode('utf-8'),
                        "callback_data": f"mc_{beep_id}_{callback_text}_{question_id}"
                    })

            # Добавляем строку в клавиатуру
            raw_keyboard.append(row)

        # Добавляем две конпки: для отрпавки ответов и очистки выбора
        clean_row = [{
                "text": "🧹 Очистить выбор",
                "callback_data": f"mc_{beep_id}_clean"
                }]
        
        send_row = [{
                "text": "🚀 Отправить ответ",
                "callback_data": f"mc_{beep_id}_send"
                }]
        raw_keyboard.extend([clean_row, send_row])

        # Формируем клавиатуру
        keyboard = json.dumps({"inline_keyboard": raw_keyboard})
    
    elif question_type == "location":
        
        # Оставляем только два первых элемента
        response_options = response_options[:2]

        # Определяем количество строк и количество элементов в каждой строке
        rows_amount = 2
        buttons_per_row = 1

        # Формируем клавиатуру
        raw_keyboard = []
        for i in range(len(response_options)):
            row = []
            button = {
                "text": response_options[i].decode('utf-8')
            }
            # Первой кнопке добавляем запрос локации
            if i == 0:
                button["request_location"] = True
            row.append(button)
            raw_keyboard.append(row)

            # Формируем клавиатуру
            keyboard = json.dumps(
                {"keyboard": raw_keyboard,
                "resize_keyboard": True,
                "one_time_keyboard": True,
                "hide_keyboard": True})
            
    # Возвращаем клавиатуру
    return keyboard


# 6. Функция для формирования вопроса
def create_quest_text(question_id, question, comment, expire_time):

    # Форматируем текст сообщения
    has_expire_time = bool(expire_time)
    has_comment = comment != "nan"

    if has_expire_time:
        formatted_time = datetime.fromtimestamp(expire_time).strftime('%H:%M')
        if has_comment:
            question_text = QUESTION_TEXT_CAPTION.format(question_id, formatted_time, question, comment)
        else:
            question_text = QUESTION_TEXT.format(question_id, formatted_time, question)
    else:
        if has_comment:
            question_text = QUESTION_TEXT_WT_CAPTION.format(question_id, question, comment)
        else:
            question_text = QUESTION_TEXT_WT.format(question_id, question)

    return question_text


# 7. Функция для подготовки бипов
def prepare_beep(beep_data):

    # Получаем информаии о вопросе для отправки
    question_df = get_survey_quest(beep_data['study_id'], beep_data['question_id'], beep_data['survey'].decode('utf-8'))
    # Получаем информацию о типе вопроса
    question_type = question_df['question_type'].decode('utf-8')

    # Формируем текст для отправки
    expire_time = beep_data.get('expire_time', None)
    question_text = create_quest_text(
        question_df['question_num'],
        question_df['question'].decode('utf-8'),
        question_df['comment'].decode('utf-8'),
        expire_time)

    # Формируем клавиатуру для отправки
    keyboard = None
    if question_type in ('multiple_choice', 'single_choice', 'location'):
        keyboard = create_keyboard(
            question_df, 
            beep_data['participant_id'], 
            beep_data['beep_id'], 
            beep_data['question_id'], 
            beep_data['time_to_send'], 
            question_type
        )

    # Возвращаем id участника (куда отправляем), текст и клавиатуру опросника (что отправляем)
    return beep_data['participant_id'], question_text, keyboard


# 8. Функция для отправки бипов новым сообщением
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


# 9.2. Функция для отправки бипов в том же сообщении  
def update_message(user_id, message_id, question_id, old_beep_loc=False):

    next_beep = get_next_beep(user_id, message_id, question_id)

    if not next_beep:
        question_text = SURVEY_COMPLETE_MESSAGE
        edit_question_message(user_id, message_id, question_text) 
        update_particip_by_id(user_id,  {'status': "participating"})
        
    else:
        try:
            expire_time = next_beep.get('expire_time', None)
            question_df = get_survey_quest(next_beep['study_id'], next_beep['question_id'], next_beep['survey'].decode('utf-8'))
            question_text = create_quest_text(
                question_df['question_num'],
                question_df['question'].decode('utf-8'),
                question_df['comment'].decode('utf-8'),
                expire_time)

            # Формируем клавиатуру для отправки
            keyboard = None
            if question_df['question_type'].decode('utf-8') in ('multiple_choice', 'single_choice', 'location'):
                keyboard = create_keyboard(
                    question_df, 
                    next_beep['participant_id'], 
                    next_beep['beep_id'], 
                    next_beep['question_id'], 
                    next_beep['time_to_send'], 
                    question_df['question_type'].decode('utf-8')
                )

            # Отправка нового бипа
            # В случае локации (если запрашивалась предыдущим бипом или будет запрашиваться в нынешнем) – новым сообщением
            if old_beep_loc or question_df['question_type'].decode('utf-8') == 'location':
                send_beep(user_id, question_text, keyboard)
            # В случае вопроса или открытого – через редактирование старого сообщения
            else:
                edit_question_message(user_id, message_id, question_text, keyboard)

        except Exception as e:
            print(e)


# РАССЫЛКИ
# 11.1. Функция для формирования бипов для отправки
def form_beep_dicts(chat_id, study_id, surveys_len, days, time_to_send, expire_time, questions_types, survey):
    
    beeps_list = []
    for day in range (0, days + 1):
        for beep in range(int(surveys_len)):
            beep_dict = {}
            beep_dict['participant_id'] = chat_id
            beep_dict['study_id'] = study_id
            beep_dict['question_id'] = beep + 1
            beep_dict['time_to_send'] = time_to_send[day].strftime('%Y-%m-%dT%H:%M:%SZ') 
            beep_dict['expire_time'] = expire_time[day].strftime('%Y-%m-%dT%H:%M:%SZ')
            beep_dict['question_type'] = questions_types[beep]
            beep_dict['survey'] = survey
            beeps_list.append(beep_dict)

    return beeps_list

# 11.2. Функция для формирования бипов для отправки (стартовый опросник)
def form_beep_dicts_es(chat_id, study_id, surveys_len, time_to_send, questions_types, survey):
    
    beeps_list = []
    for beep in range(int(surveys_len)):
        beep_dict = {}
        beep_dict['participant_id'] = chat_id
        beep_dict['study_id'] = study_id
        beep_dict['question_id'] = beep + 1
        beep_dict['time_to_send'] = time_to_send.strftime('%Y-%m-%dT%H:%M:%SZ') 
        beep_dict['question_type'] = questions_types[beep]
        beep_dict['survey'] = survey
        beeps_list.append(beep_dict)
        
    return beeps_list

# 12. Функция для проверки и закрытия истекших бипов
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


# ФОРМИРОВАНИЕ РАСПИСАНИЯ
# 13.1. Всопомогательная функция для извлечения временных точек / интервалов
def extract_and_sort_times(text):
    # Находим все интервалы и одиночные времена
    matches = re.findall(r'\b\d{1,2}:\d{2}(?:-\d{1,2}:\d{2})?', text)
    
    # Функция для получения времени начала для сортировки
    def get_start_time(time_str):
        start = time_str.split('-')[0]
        return datetime.strptime(start, "%H:%M")
    
    # Сортируем от большего к меньшему
    sorted_times = sorted(matches, key=get_start_time)
    
    # Объединяем в строку
    return ', '.join(sorted_times)

# 13.2. Всопомогательная функция для извлечения иинтервала прохождения опроса
def extract_time_range(text):
    # Извлекаем только значения после знака равенства
    times = list(map(int, re.findall(r'=(\d+)', text)))
    
    if not times:
        return ""
    
    min_time = min(times)
    max_time = max(times)

    return str(min_time) if min_time == max_time else f"{min_time} - {max_time}" 

# 13.3. Всопомогательная функция для извлечения значений конкретных опросов
def extract_survey_value(text, survey_name):
    pattern = rf'{re.escape(survey_name)}=([^;]+)'
    match = re.search(pattern, text)
    if match:
        return match.group(1).strip()
    return None

# 13.4. Функция для формирования расписания (для базовых опросников)
def form_schedule(chat_id, study_info, particip_info, timezone, base_surveys):

    # Форируем строку с временем прохождения опросов
    completion_tl = extract_time_range(study_info['completion_tl'].decode('utf-8'))

    # Форируем строку с временем / временными интервалами для отправки 
    prompting_time = extract_and_sort_times(study_info['prompting_time'].decode('utf-8'))

    send_message(chat_id, PARTICIP_INFO_MESSAGE.format(
        completion_tl, 
        study_info['duration'],
        prompting_time
    ))

    for survey in base_surveys:

        # Извлекаем время для отправки конкретного опроса
        prompting_time = extract_survey_value(study_info['prompting_time'].decode('utf-8'), survey)

        # Извлекаем время для прохождения конкретного опроса
        time_limit = int(extract_survey_value(study_info['completion_tl'].decode('utf-8'), survey))
                        
        # Готовим данные для загрузки в БД
        # Получаем типы вопросов
        questions_types = get_questions_types(particip_info['study_id'].decode('utf-8'), survey)

        # Получаем длину опросника
        survey_len = len(questions_types)

        # Заготовка для бипов в рамках исследования
        beep_dicts = {}

        # Проходимя по каждой временной точке / интервалу
        time_points = prompting_time.split(",")

        for time_to_send in time_points:

            # Убираем лишние пробелы
            time_to_send = time_to_send.strip()

            # Заготовки для времени
            time_to_send_arr = []
            expire_time_arr = []

            # Обрабатываем интервал
            if "-" in time_to_send:

                start_str, end_str = time_to_send.split("-")
                start_time = datetime.strptime(start_str, "%H:%M")
                end_time = datetime.strptime(end_str, "%H:%M")

                for day in range(study_info['duration'] + 1):
                    
                    # Генерируем случайное количество секунд в пределах интервала
                    random_seconds = random.randint(0, int((end_time - start_time).total_seconds()))
                    
                    # Комбинируем с текущей датой, добавляем случайные секунды и корректируем часовой пояс
                    time_to_send = datetime.combine(date.today() + timedelta(days=day), start_time.time()) + timedelta(seconds=random_seconds, hours=timezone)
                    
                    # Сохраняем сгенерированное время отправки
                    time_to_send_arr.append(time_to_send)

                    # Формируем время для закрытия бипа
                    expire_time = time_to_send + timedelta(minutes=time_limit)
                    
                    # Сохраняем время закрытия бипа
                    expire_time_arr.append(expire_time)

            # Обрабатываем одиночную временную точку
            else:
                base_time = datetime.strptime(time_to_send, "%H:%M").time()
                for day in range(study_info['duration'] + 1):
                    
                    # Комбинируем с текущей датой и добавляем часовую зону
                    time_to_send = datetime.combine(date.today() + timedelta(days=day), base_time) + timedelta(hours=timezone)
                    
                    # Сохраняем время отправки
                    time_to_send_arr.append(time_to_send)
                    
                    # Формируем и сохраняем время закрытия бипа
                    expire_time = time_to_send + timedelta(minutes=time_limit)
                    expire_time_arr.append(expire_time)

            # Формируем словари дли рассылки бипов
            beep_dicts = form_beep_dicts(
                chat_id, 
                particip_info['study_id'].decode('utf-8'), 
                survey_len, 
                study_info['duration'], 
                time_to_send_arr, 
                expire_time_arr,
                questions_types,
                survey)
            
            print(beep_dicts)

            # Загружаем данные в БД
            upload_beeps(beep_dicts)

# 13.5. Функция для формирования расписания (для входного / выходного опросников)
def form_schedule_es(chat_id, study_info, particip_info, survey, survey_type):
                        
    # Готовим данные для загрузки в БД
    # Получаем типы вопросов
    questions_types = get_questions_types(particip_info['study_id'].decode('utf-8'), survey)

    # Получаем длину опросника
    survey_len = len(questions_types)

    # Заготовка для бипов в рамках исследования
    beep_dicts = {}

    # Берем текущие время и прибавляем часовой пояс
    time_to_send = datetime.now() + timedelta(hours=UTC_SHIFT)

    # Формируем словари дли рассылки бипов
    beep_dicts = form_beep_dicts_es(
        chat_id, 
        particip_info['study_id'].decode('utf-8'), 
        survey_len, 
        time_to_send, 
        questions_types,
        survey)
        
    # Загружаем данные в БД
    upload_beeps(beep_dicts, survey_type)
    

# РАБОТА С MULTIPLE CHOICE
# 14. Функция для извлечения выбранных опций
def extract_selected_options(rows):

    # Заготовка для выбранных опций
    selected_options = []
    # Проверяем каждый вариант – если он выбран, записываем его
    for row in rows:
        for option in row:
            text = option.get("text", "")
            if text.startswith("✅"):

                # Убираем символ ✅ и лишние пробелы по краям
                option_text = text[1:].strip()
                if option_text: 
                    selected_options.append(option_text)
    
    # Формируем строку в удобном формате
    print(selected_options)
    if selected_options:
        return True, f"[{'; '.join(selected_options)}]"
    else:
        return False, "[empty]"

# 15. Функция для обнуления выбранных опций
def clean_selected_options(rows):

    # Очищаем варианты
    for row in rows:
        for option in row:
            text = option.get("text", "")
            if text.startswith("✅"):
                option["text"] = "⬜️ " + text[1:].strip()
    
    # Формируем клавиатуру из очищенных вариантов
    keyboard = json.dumps({"inline_keyboard": rows})
    return keyboard

# 16. Функция для обновления сообщения при очистке клавиатуры
def handle_mc_clean_text(chose_anything, message_text):

    changes = False

    if chose_anything:
        message_text = message_text + f"%0A%0A_{KEYBOARD_CLEARED}_"
        changes = True
    else:

        if KEYBOARD_CLEARED in message_text:
            message_text = message_text.replace(KEYBOARD_CLEARED, "")
            message_text = re.sub(r'(%0A)+$', '', message_text.strip())
            changes = True
        
        if CANNOT_CLEAR_KEYBOARD not in message_text:
            message_text = message_text + f"_{CANNOT_CLEAR_KEYBOARD}_"
            changes = True

    return message_text, changes

# 17. Функция для обновления сообщения при очистке клавиатуры
def remove_mc_clean_text(message_text):

    for keyword in (CANNOT_CLEAR_KEYBOARD, KEYBOARD_CLEARED):
        message_text = message_text.replace(keyword, '')

    message_text = re.sub(r'(%0A)+$', '', message_text)

    return message_text

# 18. Функция для обработки выбора
def handle_mc_choice(chosen_option, rows):
    
    print(rows)

    # Проверяем каждый вариант – если он выбран, записываем его
    for row in rows:
        for option in row:

            cb_data = option["callback_data"]
            text = option["text"]

            if chosen_option == cb_data:

                # Меняем ⬜️ на ✅ и наоборот
                if text.startswith("⬜️"):
                    option["text"] = "✅" + text[1:].strip()
                elif text.startswith("✅"):
                    option["text"] = "⬜️" + text[1:].strip()

    # Обновляем клавиатуру
    keyboard = json.dumps({"inline_keyboard": rows})
    
    return rows, keyboard

# ФОРМАТИРОВАНИЕ
# 19. Применение стиля к сообщению
def apply_formatting(text, formatting):
    
    formatting = sorted(formatting, key=lambda x: x['offset'] + x['length'], reverse=True)
    
    for f in formatting:
        start = f['offset']
        end = start + f['length']
        if f['type'] == 'italic':
            text = text[:start] + '_' + text[start:end] + '_' + text[end:]
        elif f['type'] == 'bold':
            text = text[:start] + '*' + text[start:end] + '*' + text[end:]
    return text

# ПРОВЕРКА
# 20. Функция для проверки текстового ввода
def check_text_input(text_input):

    if text_input == '[no text]':
        correct = False
        upd_input = None
    else:
        correct = True
        upd_input = text_input.strip()

    # Возвращаем корректность ввода и обновленный текст
    return correct, upd_input

# ФОРМАТИРОВАНИЕ КОЛБЭКоВ
def sanitize_callback_data(text: str, max_bytes: int = 30):
    
    # 1. Удаление эмодзи
    text = re.sub(
        r'[\U0001F600-\U0001F64F'  # emoticons
        r'\U0001F300-\U0001F5FF'  # symbols & pictographs
        r'\U0001F680-\U0001F6FF'  # transport & map symbols
        r'\U0001F1E0-\U0001F1FF'  # flags (iOS)
        r'\U00002700-\U000027BF'  # dingbats
        r'\U0001F900-\U0001F9FF'  # supplemental symbols
        r'\U00002600-\U000026FF'  # miscellaneous symbols
        r'\U0001FA70-\U0001FAFF'  # extended symbols
        r'\U000025A0-\U000025FF]+',  # geometric shapes
        '', text, flags=re.UNICODE
    )

    # 2. Трим пробелов
    text = text.strip()

    # 3. Обрезка по байтам
    encoded = text.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text
    else:
        # посимвольно обрезаем до лимита по байтам
        result = ''
        byte_len = 0
        for char in text:
            char_bytes = len(char.encode('utf-8'))
            if byte_len + char_bytes > max_bytes:
                break
            result += char
            byte_len += char_bytes
        return result

