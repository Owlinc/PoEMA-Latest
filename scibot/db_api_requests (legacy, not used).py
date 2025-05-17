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


# 2. Функция для инициации исследования
def initaite_study(participant_id, study_name):

    # Запрос для создания исследования
    data = {
        'study_id': f'{participant_id}', 
        'name': f'{study_name}',
        'status': 'initiated',
    }
    
    requests.post(url=STUDY_CREATE_URL, json=data)


# 3. Функция для обновления исследования
def update_study(participant_id, update_dict):

    # Запрос для обновления исследования
    url = STUDY_UPDATE_URL + f'{participant_id}'
    print(url)
    requests.patch(url, json=update_dict)


# 4. Функция для удаления исследования
def delete_study(participant_id):

    # Запрос для удаления исследования
    url = STUDY_DELETE_URL + f'{participant_id}'
    json_results = requests.delete(url)
    results = json_results.json()

    # Возвращаем результаты
    return results


# 5. Функция для инициации участия
def initaite_particip(particip_username, study_id):

    # Запрос для создания исследования
    data = {
        'particip_username': f'{particip_username}', 
        'study_id': f'{study_id}',
        'status': 'awaited',
    }
    
    requests.post(url=PARTICIP_CREATE_URL, json=data)


# 6. Функция для обновления участия
def update_particip(particip_username, study_id, status):

    # Запрос для создания исследования
    data = {
        'particip_username': f'{particip_username}', 
        'study_id': f'{study_id}',
        'status': f'{status}',
    }
    
    requests.patch(url=PARTICIP_UPDATE_URL, json=data)


# 7. Функция для извлечения информации об участии
def get_particip_info(particip_username):

    # Запрос для получения данных об исследовании
    url = PARTICIP_INFO_URL + f'{particip_username}'
    json_results = requests.get(url)
    results = json_results.json()

    # Возвращаем результаты
    return results
