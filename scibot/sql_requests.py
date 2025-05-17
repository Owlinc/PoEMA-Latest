# БИБЛИОТЕКИ
import time
import re
import ydb
from datetime import datetime, timedelta
import pandas as pd
import random
import string

# ДРУГИЕ СКРИПТЫ
from config import *
# from utils import *
from yandex_gpt_handler import *

# КОНФИГ
REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')

# ОБЩИЕ ФУНКЦИИ
# 1. Получение ответов респондентов
def get_beeps_data(participant_id):
    
    sql_request = f"""
        SELECT * FROM {BEEPS_TABLE} 
        WHERE study_id = '{participant_id}' and closed = true LIMIT 1000;
        """
    ydb_raw_output = POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_request,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )

    # Проверяем наличие записей
    rows_list = []
    for row in ydb_raw_output[0].rows:
        data_dict = {}
        # Меняем формат бинарных данных
        for key in row:
            
            value = row[key]
            print(key, type(value))
            
            if isinstance(value, bytes):
                data_dict[key] = value.decode('utf-8')
            elif "time" in key:
                print(key, value)
                try:
                    data_dict[key] = datetime.fromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    data_dict[key] = value
            else:
                data_dict[key] = row[key]
        rows_list.append(data_dict)

    # Формируем датафрейм
    ydb_df = pd.DataFrame(rows_list)
    
    if not ydb_df.empty:
        # Выгружаем файл с записями в хранилище
        s3.put_object(
            Body=ydb_df.to_csv(index=None),
            Bucket=BACKET_NAME, 
            Key=EXPORT_FILE_NAME.format(participant_id))

        # Получаем ссылку на файл
        file_url = EXPORT_LINK + EXPORT_FILE_NAME.format(participant_id)
        return True, file_url, BEEPS_PRESCENCE.format(file_url)

    else:
        return False, None, NO_BEEPS


# 2. Проведение атрибутного сентиментного анализа
def analyse_opens_sql(chat_id):

    # 2.1 Получаем открытые для анализа
    query = f"""
    SELECT beep_id, answer FROM {BEEPS_TABLE}
    WHERE study_id = '{chat_id}' AND answer <> '[expected]' AND question_type == 'open';
    """

    open_answers = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows

    opens_dicts = []
    for row in open_answers:
        open_answer = {}
        open_answer['beep_id'] = row['beep_id']
        open_answer['answer'] = row['answer']
        open_answer['attr_sent_analysis'] = analyse_open(open_answer['answer'])
        opens_dicts.append(open_answer)

    # Если нет подходящих записей, то сворачиваемся и информируем пользователя об этом
    if len(opens_dicts) == 0:
        return OPEN_ATR_NO_DATA

    # 2.2 Записываем результаты анализа обратно в таблицу
    for open_dict in opens_dicts:
        time.sleep(0.15)
        update_query = f"""
        UPDATE {BEEPS_TABLE}
        SET 
            attr_sent_analysis = '{open_dict['attr_sent_analysis']}'
        WHERE 
            beep_id = {open_dict['beep_id']};
        """
        POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            update_query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        ))
    
    # 2.3 Информируем об успехе 
    return OPEN_ATR_SENT_SUCCESS


# 3. Удаление опросника
def delete_survey_sql(participant_id):

    # Очистка данных об опросе
    remove_efficiency_rq = f"""
        DELETE FROM {SURVEYS_TABLE}
        WHERE study_id = '{participant_id}';
        """
    try:
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                remove_efficiency_rq,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))

    except Exception as error:
        return error


# 4. Добавление опросника
def upload_surveys_sql(survey_df, chat_id):

    # Подготовим данные для массового вставки
    survey_df = survey_df.reset_index()
    values = []
    study_num = 0
    
    for index, row in survey_df.iterrows():
        
        # Генерируем рандомный ID
        characters = string.ascii_letters + string.digits
        str_id = f"{index:04d}_" + ''.join(random.choices(characters, k=6))

        values.append(
            f"('{str_id}', '{chat_id}', '{row['survey_name']}', {row['question_num']}, '{row['question_type']}', {row['buttons_in_row']}, "
            f"'{row['question']}', '{row['comment']}', '{row['response_1']}', '{row['response_2']}', '{row['response_3']}', "
            f"'{row['response_4']}', '{row['response_5']}', '{row['response_6']}', '{row['response_7']}', '{row['response_8']}', "
            f"'{row['response_9']}', '{row['response_10']}')"
        )

    # Осуществим массовую загрузку
    while True:
        try:
            if values:
                sql_request = f"""
                    INSERT INTO {SURVEYS_TABLE} (id, study_id, survey_name, question_num, question_type, buttons_in_row, question, comment, response_1, response_2, response_3, response_4, response_5, response_6, response_7, response_8, response_9, response_10) 
                    VALUES {', '.join(values)};
                """
                print(sql_request)
                POOL.retry_operation_sync(
                    lambda s: s.transaction().execute(
                        sql_request,
                        commit_tx=True,
                        settings=ydb.BaseRequestSettings().with_timeout(25).with_operation_timeout(25)
                    )
                )  
            break
                
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)


# ФУНКЦИИ ДЛЯ РАБОТЫ С ИССЛЕДОВАНИЕМ
# 1. Функция для получения информации об исследовании
def get_study_info(study_id):

    # Запрос для получения данных об исследовании
    sql_request = f"""
        SELECT * FROM {STUDIES_TABLE}
        WHERE study_id == '{study_id}';
        """

    results = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        sql_request,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows

    # Возвращаем результаты
    if len(results) > 0:
        return results[0]
    else:
        return None


# 2. Функция для инициации исследования
def initaite_study(participant_id, study_name):

    # Запрос для создания исследования
    insert_request = f"""
        INSERT INTO {STUDIES_TABLE} (study_id, name, status) 
        VALUES ('{participant_id}', '{study_name}', 'initiated');
        """
    POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            insert_request,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(25).with_operation_timeout(25)
        )
    )  


# 3. Функция для обновления исследования
def update_study(participant_id, update_dict):

    # Форматер значений
    def format_value(value):
        if isinstance(value, (int, float)) or 'datetime' in value:  # Для числовых
            return f"{value}"
        elif isinstance(value, str):  # Для текстовых
            return f"'{value}'"
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")  # Для непонятных
    
    # Construct the SQL update contents
    update_contents_sql = ", ".join(
        [f"{key} = {format_value(value)}" for key, value in update_dict.items()]
    )

    update_query = f"""
    UPDATE {STUDIES_TABLE}
    SET 
        {update_contents_sql}
    WHERE 
        study_id = '{participant_id}';
    """
    print(update_query)

    POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            update_query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))


# 4.1. Функция для удаления исследования
def delete_study(participant_id):

    # Очистка данных об исследовании
    remove_study = f"""
        DELETE FROM {STUDIES_TABLE}
        WHERE study_id = '{participant_id}';
        """
    try:
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                remove_study,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))
        print('study deleted')
        return True

    except Exception as error:
        return False

# 4.2. Функция для удаления информации об участниках
def delete_participation(study_id):

    # Очистка данных об исследовании
    remove_particip = f"""
        DELETE FROM {PARTICIPATION_TABLE}
        WHERE study_id = '{study_id}';
        """
    try:
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                remove_particip,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))

    except Exception as error:
        return False

# 4.3. Функция для удаления бипов участников
def delete_beeps(study_id):

    # Очистка данных об исследовании
    remove_particip = f"""
        DELETE FROM {BEEPS_TABLE}
        WHERE study_id = '{study_id}';
        """
    try:
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                remove_particip,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))

    except Exception as error:
        return False


# 5. Функция для инициации участия
def initaite_particip(particip_username, study_id):

    try:
        # Запрос для инициации участия
        insert_request = f"""
            INSERT INTO {PARTICIPATION_TABLE} (particip_username, study_id, status) 
            VALUES ('{particip_username}', '{study_id}', 'awaited');
            """
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                insert_request,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(25).with_operation_timeout(25)
            )
        ) 
    
    except Exception as error:
        return error


# 6. Функция для получения информации об опросниках исследования
def get_study_surveys(study_id):

    try:
        # Запрос для получения информации об опросниках
        request = f"""
            SELECT survey_name, MIN(id) AS id
            FROM surveys
            WHERE study_id = '{study_id}'
            GROUP BY survey_name
            ORDER BY id;
            """
        surveys = POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                request,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(25).with_operation_timeout(25)
            )
        )[0].rows

        surveys_list = []
        for i in range(0, len(surveys)):
            surveys_list.append(surveys[i]['survey_name'].decode('utf-8'))
        
        # Возвращаем лист с опросниками и количество опросников
        return surveys_list, len(surveys)
    
    except Exception as error:
        print(error)
        return error
