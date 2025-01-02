# БИБЛИОТЕКИ
import time
import re
import ydb
from datetime import datetime, timedelta

# ДРУГИЕ СКРИПТЫ
from config import *

# КОНФИГ
REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')

# ОБЩИЕ ФУНКЦИИ
# 1. Функция для получения расписания отправок
def get_user_beeps():
    REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')
    sql_request = f"""
        SELECT * FROM {BEEPS_TABLE}
        WHERE CAST(time_to_send AS String) LIKE '{REAL_CURR_TIME}%' AND message_id IS NULL;
        """
    return POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_request,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )


# 2. Функция для получения вопросов
def get_survey_quest(study_id, question_id):
    
    sql_request = f"""
        SELECT * FROM {SURVEYS_TABLE}
        WHERE study_id == '{study_id.decode('utf-8')}' AND question_num == {question_id};
        """
    print(sql_request)
    question_raw = POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_request,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )

    question = question_raw[0].rows[0]
    return question


# 3. Функция для обновления бипа
def update_beep_db(beep_id, response, message_id):

    sql_update = f"""
        UPDATE {BEEPS_TABLE}
        SET message_id = {message_id}, answer = '{response}'
        WHERE beep_id = {beep_id};
    """
    return POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_update,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )


# 4. Функция для получения следующего бипа
def get_next_beep(user_id, message_id, question_id):

    get_sent_time_rq = f"""
        SELECT * FROM {BEEPS_TABLE}
        WHERE message_id = {message_id};
    """
    last_beep_data = POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            get_sent_time_rq,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        ))[0].rows[0]
    
    time_to_send = last_beep_data['time_to_send']
    time_to_send = datetime.fromtimestamp(time_to_send).strftime('%Y-%m-%dT%H:%M')

    get_next_beep_rq = f"""
        SELECT * FROM {BEEPS_TABLE}
        WHERE CAST(time_to_send AS String) LIKE '{time_to_send}%' AND participant_id = '{user_id}' AND question_id = {int(question_id) + 1};
    """
    try:
        next_beep = POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                get_next_beep_rq,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))[0].rows[0]

    except Exception as error:
        next_beep = None

    return next_beep


# 5. Функция для записи ID в информацию об участии
def write_particip_id(particip_id, username):

    try:
        sql_upload = f"""
            INSERT INTO {USER_IDS_TABLE} (user_id, username)
            VALUES ('{particip_id}', '{username}');
        """
        return POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                sql_upload,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            )
        )   
    except Exception as error:
        return False


# 6. Извлечение username по ID
def get_username(particip_id):

    particip_id = particip_id.decode('utf-8')
    username = f"""
        SELECT username	FROM {USER_IDS_TABLE}
        WHERE user_id = '{particip_id}';
    """
    username = POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            username,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        ))[0].rows[0]['username']
    
    return(username.decode('utf-8'))


# 7. Функция для получения длины опросника
def get_survey_len(study_id):

    get_survey_items = f"""
        SELECT * FROM {SURVEYS_TABLE}
        WHERE study_id = '{study_id}';
    """
    survey = POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            get_survey_items,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        ))[0].rows
    
    return(len(survey))


# ФОМРИРВАНИЕ БИПОВ
# 1. Функция для загрузки бипов в таблицу
def upload_beeps(beeps_dict):
    while True:
        try:
            # Получим наибольшее ID
            sql_request = f"""
                SELECT MAX(beep_id) AS beep_id FROM {BEEPS_TABLE};
                """
            max_beep_id = POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                ))[0].rows[0]['beep_id']
            if not max_beep_id:
                max_beep_id = 0
            break

        except Exception as e:
            print(f"An error occurred: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)

    # Подготовим данные для массовой вставки
    values = []
    for beep in beeps_dict:
        max_beep_id += 1
        values.append(
            f"({max_beep_id}, '{beep['participant_id']}', '{beep['study_id']}', {beep['question_id']}, '{beep['question_type']}', "
            f"'[expected]', datetime('{beep['time_to_send']}'), datetime('{beep['expire_time']}'), '[not generated]')"
        )
    print(values)

    while True:
        try:
            # Выполним массовую вставку
            if values:
                sql_request = f"""
                    INSERT INTO {BEEPS_TABLE} (beep_id, participant_id, study_id, question_id, question_type, answer, time_to_send, expire_time, attr_sent_analysis) 
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


# 2. Функция для определения типа вопроса
def get_question_type(study_id, question_id):

    while True:
        try:
            sql_request = f"""
                SELECT question_type
                FROM {SURVEYS_TABLE}
                WHERE study_id = '{study_id}' AND question_num = {question_id}
                """
            quest_type = POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                ))[0].rows[0]['question_type']
            print(quest_type)
            return quest_type
            break

        except Exception as e:
            print(f"An error occurred: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)    


# 4. Функция для проверки окончания исследования (UPD)
def check_study_end(participant_id):

    # 4.1. Получаем последнее времмя отправк для участника
    while True:
        try:
            sql_request = f"""
                SELECT MAX(expire_time) AS most_recent_expire_time
                FROM {BEEPS_TABLE}
                WHERE participant_id = '{participant_id}';
                """

            latest_beep_time = POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                ))[0].rows[0]['most_recent_expire_time']   
            break

        except Exception as e:
            print(f"An error occurred: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)

    # 4.2. Проверяем нет ли хотя бы одной строки, которая была бы равна [expected]
    while True:
        try:
            sql_request = f"""
            SELECT 
                CASE 
                    WHEN COUNT(*) > 0 THEN 1 ELSE 0 
                END AS study_end
            FROM {BEEPS_TABLE}
            WHERE expire_time = datetime('{latest_beep_time}')
            AND answer NOT LIKE '[expected]';
            """

            study_end = POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                ))[0].rows[0]['study_end']   
            break

        except Exception as e:
            print(f"An error occurred: {e}")
            print("Retrying in 1 second...")
            time.sleep(1) 

    # Возвращаем True, если у нас закончилось исследование и False – в обратном случае
    return bool(study_end)


# 5. Функция для обработки просроченных бипов (UPD)
def handle_expired_beeps():

    # 5.1. Меняем [expected] на [expired]
    REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')
    update_query = f"""
    UPDATE {BEEPS_TABLE}
    SET answer = '[expired]'
    WHERE CAST(expire_time AS String) LIKE '{REAL_CURR_TIME}%' AND answer = "[expected]";
    """
    POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        update_query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

    # 5.2. Получаем пользователей для проверки (лист)
    get_query = f"""
    SELECT DISTINCT participant_id FROM {BEEPS_TABLE}
    WHERE CAST(expire_time AS String) LIKE '{REAL_CURR_TIME}%';
    """
    users_to_check_raw = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        get_query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows
    users_to_check = []
    for user in users_to_check_raw:
        users_to_check.append(user['participant_id'].decode('utf-8'))

    # 5.3. Создаем словари для блокировки участия в опросе 
    get_query_sec = f"""
    SELECT DISTINCT participant_id, message_id
    FROM {BEEPS_TABLE}
    WHERE answer = "[expired]" AND message_id IS NOT NULL;
    """
    dicts_to_edit_raw = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        get_query_sec,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows
    dicts_to_edit = []
    for user in dicts_to_edit_raw:
        messsage_to_edit = {}
        messsage_to_edit['chat_id'] = user['participant_id'].decode('utf-8')
        messsage_to_edit['message_id'] = user['message_id']
        dicts_to_edit.append(messsage_to_edit)  

    # Возвращаем лист с пользователями для дальнейшей проверки и словарь для блокировки участия в опросе   
    return users_to_check, dicts_to_edit


# 6. Функция для обновления бипов
def update_beep_data(message_id, user_id):
    REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')
    update_query = f"""
    UPDATE {BEEPS_TABLE}
    SET message_id = {message_id}
    WHERE CAST(time_to_send AS String) LIKE '{REAL_CURR_TIME}%' AND participant_id = '{user_id}';
    """
    POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        update_query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))


# 7. Удаление предыдущих биопв
def delete_beeps(participant_id):

    # Очистка данных о бипах
    remove_beeps = f"""
        DELETE FROM {BEEPS_TABLE}
        WHERE participant_id = '{participant_id}';
        """
    try:
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                remove_beeps,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))
        print('survey deleted')
    except Exception as error:
        return False


# 8. Получение самоо последнего бипа, для которого ожидается ответ
def get_beep_to_write(participant_id):

    sql_request = f"""
        SELECT * FROM {BEEPS_TABLE}
        WHERE participant_id == "{participant_id}" AND message_id IS NOT NULL AND answer = "[expected]";
        """

    beeps = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        sql_request,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows

    # Определяем самый последний beep, который был отправлен и который ожидает ответа
    smallest_question_num = 10000
    for beep in beeps:
        if  beep['question_id'] < smallest_question_num:
            smallest_question_num = beep['question_id']
            expected_beep = beep

    beep_id = expected_beep['beep_id']
    question_id = expected_beep['question_id']
    message_id = expected_beep['message_id']
    
    return beep_id, question_id, message_id
       
