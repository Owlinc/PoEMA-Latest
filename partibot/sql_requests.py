# БИБЛИОТЕКИ
import time
import re
import ydb
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# ДРУГИЕ СКРИПТЫ
from config import *

# КОНФИГ
REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')

# ОБЩИЕ ФУНКЦИИ
# 1. Функция для получения расписания отправок
def get_user_beeps():

    while True:
        try:
            
            REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')
            sql_request = f"""
                SELECT * FROM {BEEPS_TABLE}
                WHERE 
                    (CAST(time_to_send AS String) LIKE '{REAL_CURR_TIME}%' AND message_id IS NULL)
                    OR 
                    ((enterance_beep = True OR exit_beep = True) AND message_id IS NULL)
                """
            return POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(5).with_operation_timeout(3)
                )
            )
            break

        except Exception as e:
            print(f"An error occurred at getting user beeps: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)


# 2. Функция для получения вопросов
def get_survey_quest(study_id, question_id, survey):
    
    sql_request = f"""
        SELECT * FROM {SURVEYS_TABLE}
        WHERE study_id == '{study_id.decode('utf-8')}' AND question_num == {question_id} AND survey_name = '{survey}';
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
def update_beep_db(beep_id, message_id, response=None):

    print(beep_id, message_id, response)

    CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Записываем ответ и закрываем beep
    if response:
        sql_update = f"""
            UPDATE {BEEPS_TABLE}
            SET message_id = {message_id}, answer = '{response}', closed = True, time_answered = datetime('{CURR_TIME}')
            WHERE beep_id = {beep_id};
        """
    
    # Записываем ID отправленного бипа
    else:
        sql_update = f"""
            UPDATE {BEEPS_TABLE}
            SET message_id = {message_id}
            WHERE beep_id = {beep_id};
        """       

    for wait_seconds in range(1, 15):  # от 1 до 15 секунд
        try:
            return POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_update,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                )
            )

        except Exception as e:
            print(f"An error occurred at updating beeps: {e}")
            print("Retrying...")
            time.sleep(wait_seconds)
    

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
    print(f'focal time to send: {time_to_send}')

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

    print(f'next beep defind: {next_beep}')
    return next_beep


# 5. Функция для записи ID в информацию об участии
def write_particip_id(particip_id, username):

    try:
        sql_update = f"""
            UPDATE {PARTICIPATION_TABLE}
            SET particip_id = '{particip_id}'
            WHERE particip_username = '{username}';
        """
        return POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                sql_update,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            )
        )   
    except Exception as error:
        return False


# 6. Функция для получения длины опросника
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
    
    return len(survey)

# 7. Функция для получения типов вопрсоов
def get_questions_types(study_id, survey_name):

    get_survey_items = f"""
        SELECT question_type
        FROM surveys
        WHERE study_id = '{study_id}' AND survey_name = '{survey_name}'
        ORDER BY question_num
    """

    questions = POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            get_survey_items,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        ))[0].rows
    
    questions_types = []
    for i in range(0, len(questions)):
        questions_types.append(questions[i]['question_type'].decode('utf-8'))

    return questions_types

# 8. Функция для проверки того прошел ли человек входной опрос
def check_enterance_beeps():

    while True:
        try:
            # Проверочный запрос
            get_query = f"""
            SELECT participant_id
            FROM {BEEPS_TABLE}
            GROUP BY participant_id
            HAVING 
            COUNT(*) = COUNT_IF(enterance_beep = TRUE AND closed = TRUE);
            """
            users_to_handle_raw = POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                get_query,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))[0].rows

            users_to_handle = []
            for user in users_to_handle_raw:
                users_to_handle.append(user['participant_id'].decode('utf-8'))
            return(users_to_handle)
            break
                
        except Exception as e:
            print(f"An error occurred at checking enterance beeps: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)

# 9. Функция для проверки того прошел ли человек выходной опрос
def check_exit_beeps():
    while True:
        try:
            # Проверочный запрос
            get_query = f"""
                SELECT participant_id
                FROM {BEEPS_TABLE}
                WHERE exit_beep_handled IS NULL
                GROUP BY participant_id
                HAVING 
                    COUNT_IF(exit_beep = TRUE) > 0 AND
                    COUNT_IF(exit_beep = TRUE) = COUNT_IF(exit_beep = TRUE AND closed = TRUE);
            """
            users_to_handle_raw = POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                get_query,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))[0].rows

            # Добавляем участников в базу отработки
            users_to_handle = []
            for user in users_to_handle_raw:
                users_to_handle.append(user['participant_id'].decode('utf-8'))

            # Помечаем участников отработанными
            if users_to_handle:
                
                ids_formatted = ', '.join(f"'{user}'" for user in users_to_handle)
                mark_handled_query = f"""
                    UPDATE {BEEPS_TABLE}
                    SET exit_beep_handled = TRUE
                    WHERE participant_id IN ({ids_formatted});
                """
                POOL.retry_operation_sync(
                    lambda s: s.transaction().execute(
                        mark_handled_query,
                        commit_tx=True,
                        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                    )
                )

            print(f"Закрываю бипы у: {users_to_handle}")
            return(users_to_handle)
                
        except Exception as e:
            print(f"An error occurred at checking exit beeps: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)  


# ФОМРИРВАНИЕ БИПОВ
# 1. Функция для загрузки бипов в таблицу
def upload_beeps(beeps_dict, survey_type="base"):
    
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
            print(f"An error occurred at uploading beeps: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)

    # Подготовим данные для массовой вставки
    values = []
    for beep in beeps_dict:
        max_beep_id += 1
        if survey_type == 'base':
            values.append(
                f"({max_beep_id}, '{beep['participant_id']}', '{beep['study_id']}', {beep['question_id']}, '{beep['question_type']}', "
                f"'[expected]', datetime('{beep['time_to_send']}'), datetime('{beep['expire_time']}'), '[not generated]', False, '{beep['survey']}', False, False)"
            )
        elif survey_type == 'enterance':
            values.append(
                f"({max_beep_id}, '{beep['participant_id']}', '{beep['study_id']}', {beep['question_id']}, '{beep['question_type']}', "
                f"'[expected]', datetime('{beep['time_to_send']}'), NULL, '[not generated]', False, '{beep['survey']}', True, False)"
            )       
        elif survey_type == 'exit':
            values.append(
                f"({max_beep_id}, '{beep['participant_id']}', '{beep['study_id']}', {beep['question_id']}, '{beep['question_type']}', "
                f"'[expected]', datetime('{beep['time_to_send']}'), NULL, '[not generated]', False, '{beep['survey']}', False, True)"
            )   

    while True:
        try:
            # Выполним массовую вставку
            if values:
                sql_request = f"""
                    INSERT INTO {BEEPS_TABLE} (beep_id, participant_id, study_id, question_id, question_type, answer, time_to_send, expire_time, attr_sent_analysis, closed, survey, enterance_beep, exit_beep) 
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
            print(f"An error occurred at uploading beeps: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)


# 3. Функция для проверки окончания исследования
def check_study_end(participant_id):

    # 3.1. Получаем последнее время отправки для участника
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
            
            if latest_beep_time:
                latest_beep_time = datetime.fromtimestamp(latest_beep_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                return False
            break

        except Exception as e:
            print(f"An error occurred at getting last beep time sent: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)

    print('checked once')

    # 3.2. Проверяем нет ли хотя бы одной строки, которая была бы равна [expected]
    while True:
        try:
            sql_request = f"""
            SELECT 
                CASE 
                    WHEN COUNT(*) > 0 THEN 1 ELSE 0 
                END AS study_end
            FROM {BEEPS_TABLE}
            WHERE expire_time = datetime('{latest_beep_time}')
            AND answer NOT LIKE '[expected]'
            AND participant_id = '{participant_id}'
            AND study_end_wrote is NULL
            """

            study_end = POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                ))[0].rows[0]['study_end']   
            
            print('checked two')

            # Помечаем участников отработанными
            if study_end:
                
                wrote_study_end_query = f"""
                UPDATE {BEEPS_TABLE}
                SET study_end_wrote = true
                WHERE participant_id = '{participant_id}'
                AND expire_time = datetime('{latest_beep_time}')
                """
                POOL.retry_operation_sync(
                    lambda s: s.transaction().execute(
                        wrote_study_end_query,
                        commit_tx=True,
                        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
                    )
                )
            
            # Возвращаем True, если у нас закончилось исследование и False – в обратном случае
            print(f'study_end: {study_end} for {participant_id}')
            return bool(study_end)

        except Exception as e:
            print(f"An error occurred at checking study end: {e}")
            print("Retrying in 1 second...")
            time.sleep(1) 


# 4. Функция для обработки просроченных бипов (UPD)
def handle_expired_beeps():

    # 4.1. Получаем пользователей для проверки (лист)
    REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')
    get_query = f"""
    SELECT DISTINCT participant_id FROM {BEEPS_TABLE}
    WHERE CAST(expire_time AS String) LIKE '{REAL_CURR_TIME}%' AND closed == false;
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
    if len(users_to_check) == 0:
        users_to_check = None

    # 4.2. Меняем [expected] на [expired]
    update_query = f"""
    UPDATE {BEEPS_TABLE}
    SET answer = '[expired]', closed = True
    WHERE CAST(expire_time AS String) LIKE '{REAL_CURR_TIME}%' AND closed == false;
    """
    POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        update_query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

    # 4.3. Создаем словари для блокировки участия в опросе 
    get_query_sec = f"""
    SELECT participant_id, message_id
    FROM {BEEPS_TABLE}
    WHERE answer = "[expired]" AND message_id IS NOT NULL AND CAST(expire_time AS String) LIKE '{REAL_CURR_TIME}%' 
    """
    dicts_to_edit_raw = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        get_query_sec,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows
    
    # Группируем по participant_id
    by_participant = defaultdict(list)
    for user in dicts_to_edit_raw:
        pid = user['participant_id'].decode('utf-8')
        mid = user['message_id']
        by_participant[pid].append(mid)

    dicts_to_edit = []
    for pid, mids in by_participant.items():

        # Случай 1: был только один вопрос — берем его
        print(f'len(mids): {len(set(mids))}')
        if len(set(mids)) == 1:
            dicts_to_edit.append({'chat_id': pid, 'message_id': mids[0]})
        else:
            # Случай 2: message_id встречается один раз среди нескольких — берем только уникальные
            mid_counts = Counter(mids)
            for mid, count in mid_counts.items():
                if count == 1:
                    dicts_to_edit.append({'chat_id': pid, 'message_id': mid})

    # Возвращаем лист с пользователями для дальнейшей проверки и словарь для блокировки участия в опросе   
    return users_to_check, dicts_to_edit
    

# 5.1. Функция для обновления бипов (для первого вопроса)
def update_beep_data(message_id, user_id):
    
    REAL_CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M')
    CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M:%SZ')

    update_query = f"""
    UPDATE {BEEPS_TABLE}
    SET message_id = {message_id}, time_sent = datetime('{CURR_TIME}')
    WHERE participant_id = '{user_id}' AND ((CAST(time_to_send AS String) LIKE '{REAL_CURR_TIME}%') OR (enterance_beep = True or exit_beep = True)) AND message_id is NULL
    """
    POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        update_query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

# 5.2. Функция для обновления бипов (для последующих вопросов)
def set_beep_sent(beep_id):
    
    CURR_TIME = (datetime.utcnow() + timedelta(hours=UTC_SHIFT)).strftime('%Y-%m-%dT%H:%M:%SZ')
    update_query = f"""
    UPDATE {BEEPS_TABLE}
    SET time_sent = datetime('{CURR_TIME}')
    WHERE beep_id == {beep_id};
    """
    POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        update_query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

# 6. Удаление предыдущих биопв
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


# 7.1. Получение самого последнего бипа, для которого ожидается ответ
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

    # На случай если все beep'ы отправлены
    beep_id = expected_beep.get('beep_id', None)

    if beep_id is None:
        return None, None, None, None
    
    question_id = expected_beep['question_id']
    message_id = expected_beep['message_id']
    question_type = expected_beep['question_type'].decode('utf-8')
    
    return beep_id, question_id, message_id, question_type

# 7.2. Получение информации о бипе по его ID
def get_beep_data(beep_id):
    sql_request = f"""
        SELECT * FROM {BEEPS_TABLE}
        WHERE beep_id == {beep_id};
    """

    start_time = time.time()
    timeout_seconds = 13

    while time.time() - start_time < timeout_seconds:
        try:
            result = POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings()
                        .with_timeout(4)      # таймаут операции
                        .with_operation_timeout(2)         # макс. время попытки
                )
            )

            rows = result[0].rows
            if not rows:
                print("Данных с таким beep_id не найдено.")
                return None

            beep = rows[0]
            question_id = beep['question_id']
            message_id = beep['message_id']
            question_type = beep['question_type'].decode('utf-8')

            return question_id, message_id, question_type

        except Exception as e:
            print(f"Попытка не удалась: {e}")
            time.sleep(1)

    print("Не удалось получить данные за 15 секунд.")
    return "error", "error", "error"


# 8. Изменение часового пояса бипов
def change_beeps_tz(participant_id, change_value):

    change_value = -change_value
    sign = '+' if change_value >= 0 else '-'
    abs_value = abs(change_value)
    print(change_value, sign, abs_value)

    sql_request = f"""
        UPDATE beeps
        SET 
            time_to_send = time_to_send {sign} DateTime::IntervalFromHours({abs_value}),
            expire_time = expire_time {sign} DateTime::IntervalFromHours({abs_value})
        WHERE 
            enterance_beep = false
            AND exit_beep = false
            AND participant_id = '{participant_id}'
            AND answer = "[expected]"
            AND time_to_send >= CurrentUtcTimestamp();
        """

    print(sql_request)
    POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_request,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        ))
       

# УЧАСТИЕ В ИССЛЕДОВАНИИ
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


# 2. Функция для проверки наличия респондента в базе
def check_particip_studies(particip_username):

    # Запрос для получения данных об исследовании
    sql_request = f"""
        SELECT COUNT(*) AS count FROM {PARTICIPATION_TABLE} WHERE particip_username = '{particip_username}';
        """

    results = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        sql_request,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows[0]['count']

    # Возвращаем результаты
    if results == 0:
        return False
    else:
        return True


# 3.1. Функция для обновления участия (по юзернему)
def update_particip(particip_username, update_dict):

    # Форматер значений
    def format_value(value):
        if isinstance(value, (int, float)):  # Для числовых
            return f"{value}"
        elif isinstance(value, str):  # Для текстовых
            return f"'{value}'"
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")  # Для непонятных

    # Construct the SQL update contents
    update_contents_sql = ", ".join(
        [f"{key} = {format_value(value)}" for key, value in update_dict.items()]
    )

    # Запрос для обнолвения участия
    sql_update = f"""
        UPDATE {PARTICIPATION_TABLE}
        SET {update_contents_sql}
        WHERE particip_username = '{particip_username}';
    """
    POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_update,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )


# 3.2. Функция для обновления участия (по ID)
def update_particip_by_id(particip_id, update_dict):

    # Форматер значений
    def format_value(value):
        if isinstance(value, (int, float)):  # Для числовых
            return f"{value}"
        elif isinstance(value, str):  # Для текстовых
            return f"'{value}'"
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")  # Для непонятных

    # Construct the SQL update contents
    update_contents_sql = ", ".join(
        [f"{key} = {format_value(value)}" for key, value in update_dict.items()]
    )

    # Запрос для обнолвения участия
    sql_update = f"""
        UPDATE {PARTICIPATION_TABLE}
        SET {update_contents_sql}
        WHERE particip_id = '{particip_id}';
    """
    
    while True:
        try:

            POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
            sql_update,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)))
            break

        except Exception as e:
            print(f"An error occurred at updating participant by id: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)
    

# 4. Функция для извлечения информации об участии
def get_particip_info(particip_data, particip_id=False):

    # Запрос для получения данных об исследовании
    if particip_id:

        sql_request = f"""
        SELECT * FROM {PARTICIPATION_TABLE}
        WHERE particip_id == '{particip_data}';
        """

    else:
        sql_request = f"""
        SELECT * FROM {PARTICIPATION_TABLE}
        WHERE particip_username == '{particip_data}';
        """

    while True:
        try:

            results = POOL.retry_operation_sync(
                lambda s: s.transaction().execute(
                    sql_request,
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))[0].rows
            break

        except Exception as e:
            print(f"An error occurred at getting particpant info: {e}")
            print("Retrying in 1 second...")
            time.sleep(1)

    # Возвращаем результаты
    if len(results) > 0:
        return results[0]
    else:
        return None

    
# 5. Функция для удаления участника
def delete_particip(particip_username):

    # Удаление участника
    remove_beeps = f"""
        DELETE FROM {PARTICIPATION_TABLE}
        WHERE particip_username = '{particip_username}';
        """
    try:
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                remove_beeps,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))

        # Возвращаем результаты
        print('participant deleted')
        return True

    except Exception as error:

        # Возвращаем результаты
        return False


# ДАННЫЕ УЧАСТНИКА
# 1. Функция для получения информации об исследовании
def get_particip_settings(particip_id):

    # Запрос для получения данных об исследовании
    sql_request = f"""
        SELECT * FROM {PARTICIPANTS_TABLE}
        WHERE particip_id == '{particip_id}';
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

# 2. Функция для проверки наличия настроек
def check_particip_settings(particip_id):

    # Запрос для получения данных об исследовании
    sql_request = f"""
        SELECT COUNT(*) AS count FROM {PARTICIPANTS_TABLE} WHERE particip_id = '{particip_id}';
        """

    results = POOL.retry_operation_sync(
    lambda s: s.transaction().execute(
        sql_request,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))[0].rows[0]['count']

    # Возвращаем результаты
    if results == 0:
        return False
    else:
        return True

# 3. Запись настроек участника
def write_particip_settings(variables, values):

    # Запрос для записи данных
    sql_update = f"""
        REPLACE INTO {PARTICIPANTS_TABLE} ({variables})
        VALUES ({values});
    """
    print(sql_update)
    POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_update,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )


# 4. Обновление настроек участника
def update_particip_settings(participant_id, variables, values):

    set_clause = ", ".join(f"{var} = {val}" for var, val in zip(variables, values))

    # Запрос для записи данных
    sql_update = f"""
        UPDATE {PARTICIPANTS_TABLE}
        SET {set_clause}
        WHERE particip_id = '{participant_id}';
    """
    
    print(sql_update)
    POOL.retry_operation_sync(
        lambda s: s.transaction().execute(
            sql_update,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    )


# 5. Удаление настроек участника
def delete_particip_settings(particip_id):

    # Удаление участника
    remove_beeps = f"""
        DELETE FROM {PARTICIPANTS_TABLE}
        WHERE particip_id = '{particip_id}';
        """
    try:
        POOL.retry_operation_sync(
            lambda s: s.transaction().execute(
                remove_beeps,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))
            
        # Возвращаем результаты
        print('participants settings deleted')
        return True

    except Exception as error:

        # Возвращаем результаты
        return False

# 6. Увеличение кол-ва изменение часовой зоны
def tz_changes_increase(participant_id):

    # Получаем текущее кол-во изменений часовой зоны
    tz_changes = get_particip_settings(participant_id)['tz_changes']
    print(tz_changes)

    # Увеличиваем кол-во изменений на 1
    tz_changes += 1

    # Записываем в базу
    update_particip_settings(participant_id, ["tz_changes"], [tz_changes])
