# LIBRARIES
import requests
import json

# OTHER SCRIPTS
from config import *

# ОБЩИЕ ФУНКЦИИ
# 1. Функция для получения информации об исследовании
def get_study_info(participant_id):

    # Запрос для получения данных об исследовании
    url = STUDY_INFO_URL + f'{participant_id}'
    json_results = requests.get(url)
    results = json_results.json()

    # Возвращаем результаты
    return results


# 2. Функция для обновления участия
def update_particip(particip_username, status):

    # Запрос для создания исследования
    url = PARTICIP_UPDATE_URL + particip_username
    data = {
        'status': f'{status}',
    }
    
    requests.patch(url=url, json=data)


# 3. Функция для извлечения информации об участии
def get_particip_info(particip_username):

    # Запрос для получения данных об исследовании
    url = PARTICIP_INFO_URL + f'{particip_username}'
    json_results = requests.get(url)
    results = json_results.json()

    # Возвращаем результаты
    return results


# 4. Функция для удаления участника
def delete_particip(particip_username):

    # Запрос для удаления участника
    url = PARTICIP_DELETE_URL + f'{particip_username}'
    json_results = requests.delete(url)
    results = json_results.json()

    # Возвращаем результаты
    return results
