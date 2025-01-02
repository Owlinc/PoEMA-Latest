# LIBRARIES
import os
import ydb
import json

# CREDENTIALS
# Bot
TG_TOKEN = os.environ.get('TG_TOKEN')
URL = f"https://api.telegram.org/bot{TG_TOKEN}/"

# DATABASE
DRIVER = ydb.DriverConfig(
    endpoint=os.getenv('YDB_ENDPOINT'), 
    database=os.getenv('YDB_DATABASE'),
    credentials=ydb.iam.MetadataUrlCredentials(),
)
DRIVER = ydb.Driver(DRIVER)
DRIVER.wait(fail_fast=True, timeout=5)
POOL = ydb.SessionPool(DRIVER)

# DB API URL
# Создание исследования
STUDY_INFO_URL = "https://d5dq3jas75lngluat4jm.4b4k4pg5.apigw.yandexcloud.net/study_info/"
PARTICIP_UPDATE_URL = "https://d5dq3jas75lngluat4jm.4b4k4pg5.apigw.yandexcloud.net/update_particip/"
PARTICIP_INFO_URL = "https://d5dq3jas75lngluat4jm.4b4k4pg5.apigw.yandexcloud.net/particip_info/"
PARTICIP_DELETE_URL = "https://d5dq3jas75lngluat4jm.4b4k4pg5.apigw.yandexcloud.net/delete_particip/"

# TABLES
STUDIES_TABLE = "studies"
BEEPS_TABLE = "beeps"
PARTICIPATION_TABLE = "participation"
SURVEYS_TABLE = "surveys"
USER_IDS_TABLE = "id_username_link"

# ВРЕМЯ
# Поправка на часовой пояс
UTC_SHIFT = 3

# КЛАВИАТУРЫ
# Для подключения к исследованию
def join_keyboard():
    return json.dumps({
                "inline_keyboard": [
            [
                {"text": "Подключиться!", "callback_data": "particip_join"}
            ]
        ]
    })

# Для подписания согласия
def agreement_keyboard():
    return json.dumps({
                "inline_keyboard": [
            [
                {"text": "Согласен", "callback_data": "particip_agree"},
                {"text": "Не согласен", "callback_data": "particip_disagree"}
            ]
        ]
    })


# TEXTS
# Для команды /start
START_MESSAGE = """
*Привет! 👋*

Бот позволит поучаствовать в EMA и дневниковых исследованиях.

Для того, чтобы подключиться к исследованию, запустив команду /find\\_my\\_study. Это позволит найти исследование, на которое вас записали и подключиться к нему.
"""

# Если не удалось найт исследования для участника
NOT_INVITED_MESSAGE = """
😢 *Вас нет в списочках...* 

К сожаланию, не смогли найти исследование, где вы были бы зарегистрированы. Если считаете, что это ошибка, решите вопрос с исследователями и поробуйте найти исследование ещё раз, запустив команду /find\\_my\\_study.
"""

# Если не удалось найт исследования для участника
ALREADY_HERE_MESSAGE = """
🚫 *Ошибка!* 

Нельзя подключиться к исследованию, т.к. вы уже подключены.
"""

# Информация об исследовании
STUDY_INFO_MESSAGE = """
✨ *Отлично! Нашли ваше исследование*

{}

{}
"""

# Заголовок согласия
AGREEMENT_TITLE = "*📄 Пользовательское соглашения*"

# Текст запроса согласия
AGREEMENT_PROMPT = """
✨ *Супер! Вы подключилсь к исследованию*

Чтобы начать участие в исследовании остался один шаг – пользовательское соглашение.

Пожалуйста, онакомьтесь, с пользовательским соглашением выше и решите согласны ли в на участие в исследовании или нет.
"""

# Если участник, не согласился
PARTICIP_INFO_MESSAGE = """
✨ *Готовьтесь! Сегодня-завтра пойдут опросы....*

Информация о рассылках:
• *Время на заполнение опроса*: {} мин.
• *Кол-во дней рассылки опроса*: {}
• *Временные точки прохождения опроса*: {}
""" 

# Если участник, не соглсился
BYE_BYE_MESSAGE = """
🥀 *До свидания....*

Мы удалили вас из базы исследования. Если вы ошиблись, свяжитесь с руководителем исследования и попросите добавить вас заново.
""" 

# Если участник пытается покинуть исследование, но его нет в списках# Если не удалось найт исследования для участника
NOWHERE_TO_LEAVE_MESSAGE = """
🚫 *Ошибка!* 

Вы и так не числитесь ни в одном из исследований.
"""
 
# Шаблон вопроса
QUESTION_TEXT_CAPTION = """
_Вопрос №{} • Ответьте до {}_ 

*{}*
 
_{}_
"""

# Шаблон вопроса
QUESTION_TEXT = """
_Вопрос №{} • Ответьте до {}_ 

*{}*
"""

# Сообщение, отоброжаемое при блокировке опроса
SURVEY_EXPIRE_MESSAGE = """
*Время опроса истекло*
Вы не ответили на часть вопросов
"""

# Финальное сообщение
THE_END_MESSAGE = """
🥀 *Это был последний опрос, исследование завершено...*

Спасибо за участие и ждем вас в следующем исследовании!
"""

# Если человек хочет подключиться к исследованию, когда он его уже прошел
ALREAY_PARTICIPATED_INFO = """
🚫 *Вы не можете подключиться к исследованию.*

Вы в нем уже поучаствовали.
"""

# Для прохождения опроса
SURVEY_COMPLETE_MESSAGE = """
_Опрос_ 

*Вы прошли опрос! 😎👍*
"""

# Общая информация о боте
HELP_MESSAGE = """
*ℹ️ Информация о боте*

*Бот позволит:*
- Найти исследование, на которое вас зарегистрировали организаторы
- Подключиться к исследованию и подписать пользовательское соглашение
– Принять участие в исследовании, отвечая на ежедневные опросы
- Выйти из исследования и удалить свои ответы

*Команды:*
- /find\\_my\\_study – найти исследование, на которое вас зарегистрировали организаторы
- /leave\\_study – покинуть исследование и удалить свои ответы
"""

