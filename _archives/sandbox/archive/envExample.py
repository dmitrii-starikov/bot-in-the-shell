import os

"""Скрипт для демонстрации вывода переменных окружения и генерации документации"""
def getEnvVars():
 """функция вывода переменных окружения"""
 #print(os.environ)
 print(os.environ['API_KEY'])
 print(os.environ['SECRET_KEY'])

def argsCheck(a, b):
 """функция выводит 2 строки с передаваемыми аргументами"""
 print(a)
 print(b)