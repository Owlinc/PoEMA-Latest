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
from utils import *
from yandex_gpt_handler import *

# КОНФИГ
REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')

# ОБЩИЕ ФУНКЦИИ
# 1. Получение ответов респондентов
def get_beeps_data(participant_id):
    
    sql_request = f"""
        SELECT * FROM {BEEPS_TABLE}
        WHERE study_id = '{participant_id}'; ;
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
            if isinstance(row[key], bytes):
                data_dict[key] = row[key].decode('utf-8')
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
        print("no_data!")
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

    # Очистка данных об эффективности расписаний
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
        print('survey deleted')
    except Exception as error:
        return False


# 4. Добавление опросника
def upload_survey_sql(survey_df, chat_id):

    # Подготовим данные для массового вставки
    survey_df = survey_df.reset_index()
    values = []
    for index, row in survey_df.iterrows():
        
        # Генерируем рандомный ID
        characters = string.ascii_letters + string.digits
        str_id = ''.join(random.choices(characters, k=8))
        values.append(
            f"('{str_id}', '{chat_id}', {row['question_num']}, '{row['question_type']}', {row['rows_amount']}, "
            f"'{row['question']}', '{row['comment']}', '{row['response_1']}', '{row['response_2']}', '{row['response_3']}', "
            f"'{row['response_4']}', '{row['response_5']}', '{row['response_6']}', '{row['response_7']}', '{row['response_8']}', "
            f"'{row['response_9']}', '{row['response_10']}')"
        )

        print(values)

    # Осуществим массовую загрузку
    while True:
        try:
            if values:
                sql_request = f"""
                    INSERT INTO {SURVEYS_TABLE} (id, study_id, question_num, question_type, rows_amount, question, comment, response_1, response_2, response_3, response_4, response_5, response_6, response_7, response_8, response_9, response_10) 
                    VALUES {', '.join(values)};
                """
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
