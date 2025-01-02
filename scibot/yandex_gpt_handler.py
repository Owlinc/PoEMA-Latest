# LIBRARIES
import requests
import json

# OTHER SCRIPTS
from config import *

# Запрос для получения атрибутного сентиметного анализа открытого ответа  
def analyse_open(open_answer):

    # Запрос для создания исследования
    data = {
        'open_answer': f'{open_answer}'
    }
    
    # Возвращаем результат анализа
    json_results = requests.post(url=YA_GPT_URL, json=data)
    return json_results.json()['result']['alternatives'][0]['message']['text']
  
