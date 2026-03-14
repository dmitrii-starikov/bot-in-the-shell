from .loggerClass import loggerClass
import os
import psycopg2
import json


class postgresLoggerClass(loggerClass):
    connection = None
    cursor = None
    dbParams = {
        'dbname': os.environ['POSTGRES_DB'],
        'user': os.environ['POSTGRES_USER'],
        'password': os.environ['POSTGRES_PASSWORD'],
        'host': os.environ['POSTGRES_HOST'],
        'port': os.environ['POSTGRES_PORT']
    }

    def __init__(self):
        self.connection = psycopg2.connect(**self.dbParams)
        self.cursor = self.connection.cursor()
        print('Init postgres logger')

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    @staticmethod
    def isSuitable(names: list) -> bool:
        return 'postgres' in names

    def log(self, level: str, message: str, context: dict, time: str) -> None:
        if level not in self.loggedStatuses:
            return None
        try:
            self.cursor.execute("INSERT INTO public.logs (data, level, message, timestamp) VALUES (%s, %s, %s, %s)",
                        (json.dumps(context), level, message, time))
            self.connection.commit()
        except Exception as e:
            print(f"{self.color_red}Exception in postgresLoggerClass! {e}{self.endof}")
            self.connection.rollback()
