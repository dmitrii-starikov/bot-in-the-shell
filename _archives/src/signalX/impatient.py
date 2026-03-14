from .signalClass import SignalClass


class impatient(SignalClass):

    @staticmethod
    def isSuitable(names: list) -> bool:
        return __class__.__name__ in names

    def setSignalName(self):
        self.signalName = __class__.__name__

    def getWeightedForecast(self, time: str) -> dict:
        answer = self.signalAnswer.copy()
        answer['SELL'] = 1.0 * self.signalWeight
        return answer