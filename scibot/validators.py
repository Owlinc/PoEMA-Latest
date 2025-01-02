# LIBRARIES
import re
import pandas as pd
import openpyxl

# OTHER SCRIPTS
from config import *

# ВАЛИДАТОРЫ
# 1. Валидатор для названия
def name_validator(input_text):
    # Выделяем название + убираем пробелы до и после
    study_name = input_text.replace("/create_study", "", 1).strip()

    # Проверяем название
    correct = 4 <= len(study_name) <= 60

    # Формируем текст сообщения
    if correct:
        message_text = CORRECT_NAME
    else:
        message_text = WRONG_NAME

    # Возвращаем текст и корректност навзвания
    return correct, message_text, study_name


# 2. Валидатор для описания
def description_validator(description):

    # Проверяем название
    correct = 20 <= len(description) <= 1000

    # Формируем текст сообщения
    if correct:
        message_text = CORRECT_DESCRIPTION
    else:
        message_text = WRONG_DESCRIPTION

    # Возвращаем текст и корректность описания
    return correct, message_text.format(SURVEY_TEMPLATE_FILE_NAME), description


# 3. Валидатор для опросника
def survey_validator(url):
    try:

        # Настраиваем ожидаемый тип данны для колонок с вариантами ответов – String
        columns_as_strings = [f"response_{i}" for i in range(1, 11)]

        # Загружаем данные из xlsx в DataFrame
        df = pd.read_excel(url, dtype={col: str for col in columns_as_strings})
        
        # Обязательные колонки
        required_columns = [
            "question_num", "question_type", "rows_amount", "question", "comment",
            "response_1", "response_2", "response_3", "response_4", "response_5",
            "response_6", "response_7", "response_8", "response_9", "response_10"
        ]
        
        # Проверяем наличие всех необходимых колонок
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return False, None, f"Отсутствуют колонки: {', '.join(missing_columns)}"
        
        # Проверки на значения
        if not pd.api.types.is_numeric_dtype(df["question_num"]) or df["question_num"].isnull().any():
            return False, None, "Колонка 'question\_num' должна содержать числовые значения и не может быть пустой"
        
        if not df["question_type"].isin(["open", "single_choice"]).all():
            return False, None, "Колонка 'question\_type' должна содержать только значения 'open' или 'single\_choice' и не может быть пустой"
        
        if not (1 <= len(df) <= 22):
            return False, None, "Количество вопросов должно находиться в диапозоне от 1 до 22"
        
        df["rows_amount"] = df["rows_amount"].fillna(1).astype(int)
        if not df["rows_amount"].between(1, 10).all():
            return False, None, "Колонка 'rows\_amount' должна содержать значения от 1 до 10"
        
        if not df["question"].apply(lambda x: 12 <= len(str(x)) <= 120).all():
            return False, None, "Длина текста в колонке 'question' должна быть от 12 до 120 символов, колонка не может быть пустой"
        
        if not df["comment"].apply(lambda x: pd.isna(x) or 12 <= len(str(x)) <= 120).all():
            return False, None, "Длина значений в колонке 'comment' должна находиться в диапозоне от 12 до 120 символов"
        
        single_choice_issues = df.loc[
            (df["question_type"] == "single_choice") & 
            (df[["response_1", "response_2"]].isnull().any(axis=1))
        ]
        if not single_choice_issues.empty:
            return False, None, "Для вопроса 'single\_choice' поля 'response\_1' и 'response\_2' должны быть заполнены"

        # Cортируем датафрейм по колонке question_num и заменяем числа на порядковый
        df = df.sort_values(by='question_num', ascending=True).reset_index(drop=True)
        df['question_num'] = range(1, len(df) + 1)
        
        # Если все проверки пройдены
        return True, df, CORRECT_SURVEY
    
    except Exception as e:
        return False, None, f"Ошибка при обработке файла: {str(e)}"


# 4. Валидатор для времени прохождения опроса
def comp_tl_validator(comp_tl):

    # Проверяем корректность ввода
    try:
        number = int(comp_tl.strip())
        correct = 1 <= number <= 120
    except ValueError:
        correct = False 

    # Формируем текст сообщения
    if correct:
        message_text = CORRECT_TL.format(number)
        # Возвращаем текст и корректность времени прохождения опроса
        return correct, message_text, number
    else:
        message_text = WRONG_TL
        # Возвращаем текст и корректность времени прохождения опроса
        return correct, message_text, None


# 5. Валидатор для длительность исследования
def duration_validator(duration):

    # Проверяем корректность ввода
    try:
        number = int(duration.strip())
        correct = 2 <= number <= 90
    except ValueError:
        correct = False 

    # Формируем текст сообщения
    if correct:
        message_text = CORRECT_DURATION.format(number)
        # Возвращаем текст и корректность длительности
        return correct, message_text, number
    else:
        message_text = WRONG_DURATION
        # Возвращаем текст и корректность длительности
        return correct, message_text, None


# 6. Валидатор для участников исследования
def participants_validator(usernames):

    # Загатовки для проверки
    forbidden_chars = r"[@!{}()*^$#]"
    correct = True

     # Итерируемся по каждому участнику
    for username in map(str.strip, usernames.split(',')):
        if ' ' in username:
            error = f"Username '{username}' содержит пробелы."
            correct = False
        elif any(char in username for char in forbidden_chars):
            error = f"Username '{username}' содержит запрещённые символы."
            correct = False
        elif re.search(r'[а-яА-Я]', username):
            error = f"Username '{username}' содержит русскоязычные символы."
            correct = False
        elif not (5 <= len(username) <= 32):
            error = f"Длина username '{username}' должна быть от 5 до 32 символов."
            correct = False
    
    # Формируем текст сообщения
    if correct:
        message_text = CORRECT_PARTICIPANTS
    else:
        message_text = WRONG_PARTICIPANTS.format(error)

    # Возвращаем текст и корректность участников
    return correct, message_text
    

# 7. Валидатор для временных точек исследования
def pt_validator(input_string):

    # Разделяем строку на отдельные временные точки
    time_points = [time.strip() for time in input_string.split(",")]
    
    # Проверяем, что количество временных точек от 1 до 12
    if not (1 <= len(time_points) <= 12):
        return False, WRONG_TP_AMOUNT
    
    # Определяем регулярное выражение для проверки формата времени hh:mm
    time_pattern = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    
    # Проверяем каждую временную точку
    for time in time_points:
        if not time_pattern.match(time):
            return False, WRONG_TIME_FORMAT.format(time)
    
    return True, CORRECT_TP
    
