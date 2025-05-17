# LIBRARIES
import re
import pandas as pd
import openpyxl
from datetime import datetime

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


# 3.1. Валидатор для опросника
def survey_validator(sheet_name, df):
    
    try:
        
        # Проверяем наличие всех необходимых колонок
        missing_columns = [col for col in SURVEY_REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            return False, None, f"Отсутствуют колонки: {', '.join(missing_columns)} в опроснике {sheet_name}"
        
        # Проверки на значения
        if not pd.api.types.is_numeric_dtype(df["question_num"]) or df["question_num"].isnull().any():
            return False, None, "Колонка 'question\_num' должна содержать числовые значения и не может быть пустой (опросник {sheet_name})"
        
        if not df["question_type"].isin(["open", "multiple_choice", "single_choice", "location"]).all():
            return False, None, "Колонка 'question\_type' должна содержать только значения 'open', 'multiple\_choice', или 'single\_choice' и не может быть пустой (опросник {sheet_name})"
        
        if not (1 <= len(df) <= 26):
            return False, None, "Количество вопросов должно находиться в диапозоне от 1 до 26 (опросник {sheet_name})"
        
        df["buttons_in_row"] = df["buttons_in_row"].fillna(1).astype(int)
        if not (df["buttons_in_row"].between(1, 8).all()):
            return False, None, "Колонка 'buttons\_in\_row' должна содержать значения от 1 до 8 (опросник {sheet_name})"

        # # Список колонок c вариантами ответов
        # response_columns = [f"response_{i}" for i in range(1, 11)]

        # # Проверка того, что кол-во кнопок в строке не больше кол-ва кнопок
        # for idx, row in df.iterrows():

        #     # Считаем количество кнопок
        #     non_empty_count = row[response_columns].notna().sum()
            
        #     if row["buttons_in_row"] >= non_empty_count:
        #         return False, None, f"Количество кнопок в строке не может привышать количество вариантов ответов (вопрос №{row['question_num']}, опросник {sheet_name})"
        
        if not df["question"].apply(lambda x: 5 <= len(str(x)) <= 250).all():
            return False, None, f"Длина текста в колонке 'question' должна быть от 5 до 250 символов, колонка не может быть пустой (опросник {sheet_name})"
        
        if not df["comment"].apply(lambda x: pd.isna(x) or 5 <= len(str(x)) <= 250).all():
            return False, None, "Длина значений в колонке 'comment' должна находиться в диапозоне от 5 до 250 символов (опросник {sheet_name})"
        
        choice_issues = df.loc[
            df["question_type"].isin(("single_choice", "multiple_choice")) & 
            df[["response_1", "response_2"]].isnull().any(axis=1)
        ]
        if not choice_issues.empty:
            return False, None, "Для вопросов 'single\_choice' и 'multiple\_choice' поля 'response\_1' и 'response\_2' должны быть заполнены (опросник {sheet_name})"

        # Cортируем датафрейм по колонке question_num и заменяем числа на порядковый номер
        df = df.sort_values(by='question_num', ascending=True).reset_index(drop=True)
        df['question_num'] = range(1, len(df) + 1)
        
        # Если все проверки пройдены
        return True, df, None
    
    except Exception as e:
        return False, None, f"Ошибка при обработке опросника {sheet_name}: {str(e)}"


# 3.2. Валидатор для набора опросников
def all_surveys_validator(url):

    # Настраиваем ожидаемый тип данных для колонок с вариантами ответов – String
    columns_as_strings = {f"response_{i}": str for i in range(1, 11)}

    # Сначала открываем Excel файл через ExcelFile, чтобы получить список листов в порядке
    xls = pd.ExcelFile(url)
    sheet_order = xls.sheet_names

    # Читаем только нужные листы с сохранением порядка
    dfs = {}
    for sheet in sheet_order:
        if sheet not in ["Обозначения", "Пример", "Шаблон", "Technical Sheet"]:
            df = pd.read_excel(xls, sheet_name=sheet, dtype=columns_as_strings)
            dfs[sheet] = df

    # Проверяем каждый опросник
    surveys_dfs_list = []
    for sheet_name in dfs:
        df = dfs[sheet_name]
        valid, prepared_df, error_description = survey_validator(sheet_name, df)

        if not valid:
            return False, None, error_description, None, None, None
        else:
            prepared_df['survey_name'] = sheet_name
            surveys_dfs_list.append(prepared_df[SURVEY_REQUIRED_COLUMNS + ["survey_name"]])

    # Объединяем всё в один DataFrame
    survey_df = pd.concat(surveys_dfs_list, ignore_index=True)

    # Формируем строку с названиями опросов, соблюдая оригинальный порядок
    surveys_names = list(dfs.keys())
    surveys_amount = len(surveys_names)
    surveys_string = ", ".join(surveys_names)

    # Формируем ответ
    if surveys_amount == 1:
        response = CORRECT_SURVEY.format(surveys_string)
        keyboard = None
    else:
        response = SEVERAL_CORRECT_SURVEY.format(surveys_string)
        keyboard = enter_survey_dec_keyboard()

    return True, survey_df, response, keyboard, surveys_names, surveys_amount


# 4. Валидатор для времени прохождения опроса
def comp_tl_validator(comp_tl, base_surveys, tl_surveys):

    # Проверяем корректность ввода
    try:
        number = int(comp_tl.strip())
        correct = 1 <= number <= 120
    except ValueError:
        correct = False 

    # Если время введено корректно
    if correct:

        # Смотрим на опросники для которых уже настроено время прохождения
        if tl_surveys:
            setted_surveys = [entry.split('=')[0].strip() for entry in tl_surveys]
            tls = [entry.split('=')[1].strip() for entry in tl_surveys]
            tls.append(number)
        else:
            setted_surveys = []
            tl_surveys = []
            tls = [number]
            
        print(f"tls: {tls}")

        # Получаем опрос для настройки
        survey_to_set = next((item for item in base_surveys if item not in setted_surveys), None)

        # Добавляем опросник
        setted_surveys.append(survey_to_set)

        # Получаем следующий опрос для настройки
        next_survey = next((item for item in base_surveys if item not in setted_surveys), None)

        # Форматируем время для настраиваемого опроса
        setted_tl = f"{survey_to_set}={number}"

        # Добавляем к другим настроенным опросам
        tl_surveys.append(setted_tl)
        tl_surveys = "; ".join(str(x) for x in tl_surveys)

        # Проверяем, не является ли опросник последним или первым
        last_survey = survey_to_set == base_surveys[-1]
        first_survey = survey_to_set == base_surveys[0]

        if last_survey:
            message_text = CORRECT_TL.format(number, survey_to_set)
            keyboard = None
        # Если опрсоник первый, то высылаем предложение установить введенное время для всех опросов
        elif first_survey:
            message_text = ONE_SURVEY_TIME_DEC.format(number, survey_to_set)
            keyboard = one_duration_dec_keyboard(number)

        # В остальных случаях
        else:
            message_text = DUR_REQUEST_TEXT.format(survey_to_set, next_survey)
            keyboard = tl_choice_keyboard(tls)

        return correct, tl_surveys, message_text, keyboard, first_survey, last_survey

    else:
        message_text = WRONG_TL
        # Возвращаем текст и корректность времени прохождения опроса
        return correct, None, message_text, None, None, None


# 5. Валидатор для длительности исследования
def duration_validator(duration):

    # Проверяем корректность ввода
    try:
        number = int(duration.strip())
        correct = 1 <= number <= 90
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
    forbidden_chars = r"[!{}()*^$#]"
    correct = True
    cleaned_usernames = []

     # Итерируемся по каждому участнику
    for username in map(str.strip, usernames.split(',')):
        if username.startswith('@'):
            username = username[1:]

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

        # Приводим username к нижнему регистру
        username = username.lower()

        # Добавляем username в базу
        cleaned_usernames.append(username)
    
    # Формируем текст сообщения
    if correct:
        message_text = CORRECT_PARTICIPANTS
    else:
        message_text = WRONG_PARTICIPANTS.format(error)

    cleaned_usernames_str = ', '.join(cleaned_usernames)

    # Возвращаем текст и корректность участников
    return correct, message_text, cleaned_usernames_str


# 7.0. Вспомогательная функция для проверки ввода времени
def parse_time(time_str):
    try:
        return datetime.strptime(time_str, "%H:%M")
    except ValueError:
        return None

# 7.1. Вспомогательная функция для форматирования времени
def normalize_time_str(time_str):
    try:
        return datetime.strptime(time_str.strip(), "%H:%M").strftime("%H:%M")
    except ValueError:
        return time_str

# 7.2. Валидатор для временных точек исследования
def pt_validator(input_string):

    # Разделители для интервалов
    interval_delimiters = ["-", "–", "—"]
    delimiter_pattern = r"\s*[-–—]\s*"

    # Разделяем строку на отдельные временные точки
    time_points = [time.strip() for time in input_string.split(",")]
    
    # Проверяем, что количество временных точек от 1 до 12
    if not (1 <= len(time_points) <= 12):
        return False, WRONG_TP_AMOUNT, None
    
    # Определяем регулярное выражение для проверки формата времени hh:mm
    time_pattern = re.compile(r"^(\d{1,2}:\d{2})$" + "|" + delimiter_pattern.join(["(\d{1,2}:\d{2})"] * 2))
    
    # Проверяем временную точку / интервал
    for i, tp in enumerate(time_points):

        match = re.fullmatch(time_pattern, tp)
        if not match:
            return False, WRONG_TIME_FORMAT.format(tp), None
        
        # Проверка интервала
        if any(delim in tp for delim in interval_delimiters):

            # Выделяем стартовую и конечную точки интервала
            start_str, end_str = re.split(delimiter_pattern, tp)
            start_time, end_time = parse_time(start_str), parse_time(end_str)
            start_str, end_str = normalize_time_str(start_str), normalize_time_str(end_str)
            
            # Проверяем, явлются ли точки временем
            if not start_time or not end_time:
                return False, WRONG_TIME_FORMAT.format(tp), None
            
            # Проверяем порядок интервалов
            if start_time >= end_time:
                return False, WRONG_TIME_ORDER.format(tp), None

            # Считаем протяженность интервала
            duration = (end_time - start_time).seconds

            # Проверяем, не слишком ли короткий интервал
            if duration < 5 * 60:
                return False, TOO_SHORT_INTERVAL.format(tp), None
            
            # Проверяем, не слишком ли длинный интервал
            if duration > 3 * 3600:
                return False, TOO_LONG_INTERVAL.format(tp), None
            
            # Приводим к корректному формату
            time_points[i] = f"{start_str}-{end_str}"

        # Проверка временной точки
        else:  
            # Проверяем, является ли точка временем
            if not parse_time(tp):
                return False, WRONG_TIME_FORMAT.format(tp), None
            normalized_tp = normalize_time_str(tp)
            time_points[i] = normalized_tp

        # Формирование строки с временными точкми и интервалами
        pts = ", ".join(time_points)
    
    return True, CORRECT_TP, pts
    

# 7.2. Валидатор для нескольких опросов
def pt_validator_multi(input_string, current_pts, base_surveys):

    # Провереяем введенное время
    correct, response, pts = pt_validator(input_string)
    if not correct:
        return correct, response, pts, None, None, None

    # Смотрим на опросники для которых уже настроено время отправки
    if current_pts:
        setted_surveys = [entry.split('=')[0].strip() for entry in current_pts]
    else:
        setted_surveys = []
        current_pts = []

    # Получаем опрос для настройки
    survey_to_set = next((item for item in base_surveys if item not in setted_surveys), None)

    # Добавляем опросник
    setted_surveys.append(survey_to_set)

    # Получаем следующий опрос для настройки
    next_survey = next((item for item in base_surveys if item not in setted_surveys), None)

    # Форматируем время для настраиваемого опроса
    setted_pt = f"{survey_to_set}={pts}"

    # Добавляем к другим настроенным опросам
    current_pts.append(setted_pt)
    current_pts = "; ".join(str(x) for x in current_pts)

    # Проверяем, не является ли опросник последним
    penult_survey = survey_to_set == base_surveys[-2]

    # Возвращаем корректность, обновленный список времени для отправки и ответ
    return True, None, current_pts, survey_to_set, next_survey, penult_survey


# 8. Валидатор даты
def date_validator(input_date):

    print(input_date)
    pattern = r'/set_start_date\s+(\d{1,2})\.(\d{1,2})\.(\d{4})'
    data_match = re.search(pattern, input_date)
    
    if not data_match:
        print('no data match')
        return False, None, NO_DATE_ERROR

    try:
        print('we are here')
        day, month, year = data_match.groups()
        date_str = f"{day.zfill(2)}.{month.zfill(2)}.{year}"
        parsed_date = datetime.strptime(date_str, "%d.%m.%Y")
        start_date = f"datetime('{parsed_date.strftime('%Y-%m-%dT%H:%M:%SZ')}')"
        date_msg = parsed_date.strftime("%d.%m.%Y")
    except Exception:
        return False, None, NO_DATE_ERROR

    # Проверка свежести даты
    tomorrow = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    if parsed_date < tomorrow:
        return False, None, TOO_EARLY_DATE_ERROR
    
    return True, start_date, SUCCESS_SET_START_DATE.format(date_msg)
