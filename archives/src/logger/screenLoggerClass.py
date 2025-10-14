from .loggerClass import loggerClass
import random
import string
import datetime
import os


class screenLoggerClass(loggerClass):
    terminal_length = 88

    def __init__(self):
        print('Init screen logger')
        self.terminal_length = os.get_terminal_size().columns

    @staticmethod
    def isSuitable(names: list) -> bool:
        return 'screen' in names

    def handleMessage(self, level: str, message: str, time: str) -> str:
        length = len(message) + len(level) + len(time) + 3
        output = f"{self.bold}{level}{self.endof}: {message} {'.' * (self.terminal_length - length)}{time:>5}"
        return output

    def log(self, level: str, message: str, context: dict, time: str) -> None:
        if level not in self.loggedStatuses:
            return None
        message = self.handleMessage(level, message, time)
        # colorize
        if level == self.statuses['alert_status']:
            message = f"\033[91m{message}{self.endof}"
        elif level == self.statuses['ok_status']:
            message = f"{self.color_green}{message}{self.endof}"
        elif level == self.statuses['critical_status']:
            message = f"{self.color_red}{message}{self.endof}"
        elif level == self.statuses['error_status']:
            message = f"{self.color_red}{message}{self.endof}"
        elif level == self.statuses['warning_status']:
            message = f"{self.color_orange}{message}{self.endof}"
        elif level == self.statuses['notice_status']:
            message = f"{self.color_cyan}{message}{self.endof}"
        elif level == self.statuses['info_status']:
            message = f"{self.color_blue}{message}{self.endof}"

        print(message)
        if {} != context:
            print(str(context))
            print('*' * self.terminal_length)
