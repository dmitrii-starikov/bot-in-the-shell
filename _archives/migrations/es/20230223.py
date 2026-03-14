import os
import sys
import time
import orjson
import datetime
import requests
import numpy as np
from tqdm import tqdm
from elasticsearch import Elasticsearch

# "python migrations/es/20230223.py 2023" - загрузит в отдельные индексы информацию за 2023 год
# localhost:9201/daily_eth/_doc/2024-07-31
HISTORY_INDEX_PREFIX = 'daily_' if len(sys.argv) == 1 else 'daily_' + sys.argv[1] + '_'
TMP_FILES_DIRECTORY = '/../tmp/'

coinFirstHistoricalPrice = 0
coinFirstHistoricalVolume = 0
parsingCoin = ""

def migrate():
    directory = os.path.dirname(os.path.realpath(__file__)) + TMP_FILES_DIRECTORY
    if not os.path.exists(directory):
        os.makedirs(directory)

    es = Elasticsearch(os.environ['ELASTICSEARCH_HOST'])

    for coin in ['btc', 'eth', 'bnb']:
        requestBody = {
            "properties": {
                "timeOpen": {
                    "type":   "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "timeClose": {
                    "type":   "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "timeHigh": {
                    "type":   "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "timeLow": {
                    "type":   "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "openPrice": {
                    "type":   "float"
                },
                "highPrice": {
                    "type":   "float"
                },
                "lowPrice": {
                    "type":   "float"
                },
                "closePrice": {
                    "type":   "float"
                },
                "volume": {
                    "type":   "long"
                },
                "marketCap": {
                    "type":   "long"
                },
                ###### computable
                "avgPrice": {
                    "type":   "float"
                },
                "avgPriceComparedToYesterday": {
                    "type":   "float"
                },
                "avgPriceWeek": {
                    "type":   "float"
                },
                "avgPriceMonth": {
                    "type":   "float"
                },
                "priceVariation": {
                    "type":   "float"
                },
                "priceVariationAbs": {
                    "type":   "float"
                },
                "priceVariationWeek": {
                    "type":   "float"
                },
                "priceVariationMonth": {
                    "type":   "float"
                },
                "priceVariationComparedToWeek": {
                    "type":   "float"
                },
                "priceVariationComparedToMonth": {
                    "type":   "float"
                },
                "increaseDay": {
                    "type":   "boolean"
                },
                "differenceBTC": {
                    "type":   "float"
                },
                "LgAvgPrice": {
                    "type":   "float"
                },
                "LogAvgPrice": {
                    "type":   "float"
                },
                "LogAvgPriceWeek": {
                    "type":   "float"
                },
                "LogAvgPriceMonth": {
                    "type":   "float"
                },
                "deviationVariationWeek": {
                    "type":   "float"
                },
                "deviationVariationMonth": {
                    "type":   "float"
                },
                "deviationAvgPriceWeek": {
                    "type":   "float"
                },
                "deviationAvgPriceMonth": {
                    "type":   "float"
                },
                "LogVolume": {
                    "type":   "float"
                },
                "increasePercentWeek": {
                    "type":   "float"
                },
                "increasePercentWeightedMonth": {
                    "type":   "float"
                }
            }
        }
        if es.indices.exists(index=HISTORY_INDEX_PREFIX + coin):
            es.indices.delete(index=HISTORY_INDEX_PREFIX + coin)
        es.indices.create(index=HISTORY_INDEX_PREFIX + coin, mappings=requestBody)

    parseData(es)
def parseData(esConnection):
    global parsingCoin
    global coinFirstHistoricalPrice
    global coinFirstHistoricalVolume
    coinsList = [os.environ['COINMARKETCAP_BTC_ID'], os.environ['COINMARKETCAP_ETH_ID'], os.environ['COINMARKETCAP_BNB_ID']]
    if (len(sys.argv) == 1):
        yearsList = range(2017, 2025, 1) # 1.1.2017 - 1.1.2025
    else:
        yearsList = range(int(sys.argv[1]), int(sys.argv[1]) + 1, 1)

    for progressBar in tqdm([[{'coin': coin, 'year': y} for y in yearsList] for coin in coinsList], desc="Parse data..."):
        for iteration in progressBar:
            coin = iteration['coin']
            year = iteration['year']

            if (parsingCoin != coin):
                parsingCoin = coin
                historicalArray = np.array([])

            #print(coin, year)

            startDate = int(round(datetime.datetime(year, 1, 1, 0).timestamp()))
            endDate = int(round(datetime.datetime(year + 1, 1, 1, 0).timestamp()))

            content = ""
            if (os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + TMP_FILES_DIRECTORY + str(coin) + "_" + str(year) + '.json')):
                with open(os.path.dirname(os.path.realpath(__file__)) + TMP_FILES_DIRECTORY + str(coin) + "_" + str(year) + '.json') as file:
                    fileContent = file.read()
                    content = orjson.loads(fileContent)
                    file.close()
            else:
                url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical?id="+str(coin)+"&convertId=2781&timeStart="+str(startDate)+"&timeEnd="+str(endDate)
                urlContent = requests.get(url).content
                content = orjson.loads(urlContent)
                with open(os.path.dirname(os.path.realpath(__file__)) + TMP_FILES_DIRECTORY + str(coin) + "_" + str(year) + '.json', 'w') as file:
                    file.write(str(urlContent, 'utf-8'))
                    file.close()

            if (content['status']['error_message'] == "SUCCESS"):
                indexName = HISTORY_INDEX_PREFIX + content['data']['symbol'].lower()

                if yearsList[0] == year:
                    coinFirstHistoricalPrice = content['data']['quotes'][0]['quote']['low']
                    coinFirstHistoricalVolume = content['data']['quotes'][0]['quote']['volume']

                for archivePrice in content['data']['quotes']:

                    doc = getComputableValues(convertCoinMarketObject(archivePrice),
                                              historicalArray,
                                              esConnection if content['data']['symbol'].lower() != "btc" else None)

                    if len(historicalArray) > 30:
                        historicalArray = historicalArray[1:]

                    historicalArray = np.append(historicalArray, doc)

                    resp = esConnection.index(index=indexName, id=getDate(doc['timeOpen']), document=doc)
                    if resp['result'] != "created":
                        print("Error with record: index " + indexName + ", date " + doc['timeOpen'])
            else:
                print("Error with request: coin " + coin + ", year " + year)

            time.sleep(1)

def convertISO8601(date):
    return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f%z').strftime("%Y-%m-%d %H:%M:%S")
def getDate(date):
    return datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
def convertCoinMarketObject(json):
    return {
        "timeOpen": convertISO8601(json['timeOpen']),
        "timeClose": convertISO8601(json['timeClose']),
        "timeHigh": convertISO8601(json['timeHigh']),
        "timeLow": convertISO8601(json['timeLow']),
        "openPrice": json['quote']['open'],
        "highPrice": json['quote']['high'],
        "lowPrice": json['quote']['low'],
        "closePrice": json['quote']['close'],
        "volume": json['quote']['volume'],
        "marketCap": json['quote']['marketCap']
    }
def getComputableValues(doc, history, esConnection):
    global coinFirstHistoricalPrice
    global coinFirstHistoricalVolume

    def getAvgPrice(i):
        return i["avgPrice"]
    def getIncreaseDay(i):
        return 1 if i["increaseDay"] else 0
    def getPriceVariation(i):
        return i["priceVariation"]
    def getLogAvgPrice(i):
        return i["LogAvgPrice"]
    def countAvg(values, f):
        vectorizeNpFunction = np.vectorize(f)
        return np.average(vectorizeNpFunction(values))
    def countWeightedAvg(values, f):
        vectorizeNpFunction = np.vectorize(f)
        weights = np.arange(1 / len(values) - 0.00000001, 1, 1 / len(values))
        #print("values", values)
        #print("weights", weights, "valuesLen", len(values))
        return np.average(vectorizeNpFunction(values), weights=weights)
    def countStd(values, f):
        vectorizeNpFunction = np.vectorize(f)
        return np.std(vectorizeNpFunction(values))

    # средняя цена за сегодня
    doc["avgPrice"] = round((doc["highPrice"] + doc["lowPrice"])/2, 4)
    # колебание цены за сегодня (%), decrease < 1, increase > 1
    doc["priceVariation"] = round((doc["highPrice"]/doc["lowPrice"]) if doc["closePrice"] > doc["openPrice"] else (doc["lowPrice"]/doc["highPrice"]), 4)
    # абсолютное колебание цены за сегодня (%), без "знака"
    doc["priceVariationAbs"] = round(doc["highPrice"]/doc["lowPrice"], 4)
    # за сегодняшний день был рост или падение
    doc["increaseDay"] = doc["closePrice"] > doc["openPrice"]
    # десятичный логарифм от средней цены за сегодня
    doc["LgAvgPrice"] = round(np.log10(doc["avgPrice"]), 4)
    # логарифм* по основанию от первой исторической (в контексте скрипта) цены от средней цены за сегодня: log{b}X = log{c}X / log{c}B
    doc["LogAvgPrice"] = round(np.log(doc["avgPrice"]) / np.log(coinFirstHistoricalPrice), 4)
    # логарифм* по основанию от первого исторического (в контексте скрипта) объема торгов от сегодняшнего:
    doc["LogVolume"] = round(np.log(doc["volume"]) / np.log(coinFirstHistoricalVolume), 4)

    if len(history) > 0 and "avgPrice" in history[-1]:
        # средняя цена в сравнении с прошлым днем
        doc["avgPriceComparedToYesterday"] = round(doc["avgPrice"]/history[-1]["avgPrice"], 4)
        # средняя цена за неделю (не считая сегодняшнюю)
        doc["avgPriceWeek"] = round(countAvg(history[-7:], getAvgPrice), 4)
        # средняя цена за месяц (не считая сегодняшнюю)
        doc["avgPriceMonth"] = round(countAvg(history, getAvgPrice), 4)
        # среднее колебание цен за неделю (не считая сегодняшнюю)
        doc["priceVariationWeek"] = round(countAvg(history[-7:], getPriceVariation), 4)
        # среднее колебание цен за месяц (не считая сегодняшнюю)
        doc["priceVariationMonth"] = round(countAvg(history, getPriceVariation), 4)
        # отношение сегодняшнего колебания к среднему недельному (%)
        doc["priceVariationComparedToWeek"] = round(doc["priceVariation"]/doc["priceVariationWeek"], 4)
        # отношение сегодняшнего колебания к среднему за месяц (%)
        doc["priceVariationComparedToMonth"] = round(doc["priceVariation"]/doc["priceVariationMonth"], 4)
        # соотношение сегодняшнего колебания с BTC за сегодня (%)
        try:
            if esConnection is not None:
                resp = esConnection.get(index=HISTORY_INDEX_PREFIX + "btc", id=getDate(doc['timeOpen']))
                doc["differenceBTC"] = round(doc["priceVariation"] / resp['_source']["priceVariation"], 4)
                # todo проверить
        except Exception:
            pass
        # средний логарифм* за неделю
        doc["LogAvgPriceWeek"] = round(countAvg(history[-7:], getLogAvgPrice), 4)
        # средний логарифм* месяц
        doc["LogAvgPriceMonth"] = round(countAvg(history, getLogAvgPrice), 4)
        # среднеквадратическое отклонение колебаний за неделю
        doc["deviationVariationWeek"] = round(countStd(history[-7:], getPriceVariation), 4)
        # среднеквадратическое отклонение колебаний за месяц
        doc["deviationVariationMonth"] = round(countStd(history, getPriceVariation), 4)
        # среднеквадратическое отклонение средних цен за неделю
        doc["deviationAvgPriceWeek"] = round(countStd(history[-7:], getAvgPrice), 4)
        # среднеквадратическое отклонение средних цен за месяц
        doc["deviationAvgPriceMonth"] = round(countStd(history, getAvgPrice), 4)

    if len(history) > 7:
        # процент дней роста за предшествующую неделю
        doc["increasePercentWeek"] = round(countAvg(history[-7:], getIncreaseDay), 2)
        # взвешенный процент дней роста за предшествующий период (до месяца)
        doc["increasePercentWeightedMonth"] = round(countWeightedAvg(history, getIncreaseDay), 2)

    return doc

#if __name__ == "__main__":
migrate()