from abc import ABC, abstractmethod
class loggerClass(ABC):
    statuses = {
        'ok_status': "OK",
        'alert_status': "ALERT",
        'critical_status': "CRITICAL",
        'error_status': "ERROR",
        'warning_status': "WARNING",
        'notice_status': "NOTICE",
        'info_status': "INFO",
        'debug_status': "DEBUG"
    }
    loggedStatuses = []
    endof = '\033[0m'
    color_blue = '\033[94m'
    color_cyan = '\033[96m'
    color_green = '\033[92m'
    color_orange = '\033[93m'
    color_red = '\033[91m'
    bold = '\033[1m'

    @staticmethod
    def isSuitable(names: list) -> bool:
        return False

    @abstractmethod
    def log(self, level: str, message: str, context: dict, time: str) -> None: pass

    def setLoggedStatuses(self, statuses: list) -> None:
        if len(statuses) == 0:
            statuses = self.statuses.values()
        self.loggedStatuses = list(statuses)
        pass