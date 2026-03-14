import os
from telegram import Bot
from .loggerClass import loggerClass


class telegramLoggerClass(loggerClass):
    def __init__(self):
        self.token = os.environ['TELEGRAM_TOKEN']
        self.channel_id = os.environ['TELEGRAM_CHANNEL_ID']
        self.bot = Bot(token=self.token)
        print('Init telegram logger')

    @staticmethod
    def isSuitable(names: list) -> bool:
        return 'telegram' in names

    def log(self, level: str, message: str, context: dict, time: str) -> None:
        if level not in self.loggedStatuses:
            return None
        formatted_message = self.handleMessage(level, message, time)
        try:
            self.bot.send_message(chat_id=self.channel_id, text=formatted_message)
        except Exception as e:
            print(f"{self.color_red}Error sending message to Telegram! {e}{self.endof}")

    def handleMessage(self, level: str, message: str, time: str) -> str:
        return f"{level}: {message} {time}"
