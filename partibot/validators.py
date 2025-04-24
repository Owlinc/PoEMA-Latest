# LIBRARIES
import re
import pandas as pd
from datetime import datetime, timezone, timedelta

# OTHER SCRIPTS
from config import *

# ВАЛИДАТОРЫ
# 1. Валидатор для часа
def hour_validator(user_input):

    # Проверяем, является ли ввод числом
    if user_input.strip().isdigit():  
        hour = int(user_input)

        if 0 <= hour <= 23:
            correct = True
            moscow_time = datetime.now(timezone.utc) + timedelta(hours=3)
            timezone_offset = hour - moscow_time.hour
            if timezone_offset < -12:
                timezone_offset = 24 + timezone_offset
            
            message_text = CORRECT_HOUR
        else:
            correct = False
            timezone_offset = None
            message_text = INCORRECT_HOUR
    else:
        correct = False
        timezone_offset = None
        message_text = INCORRECT_HOUR

    # Возвращаем текст и корректность часовой зоны
    return correct, message_text, timezone_offset
