import os
import time
import json
import datetime
import requests
from tqdm import tqdm
from elasticsearch import Elasticsearch

HISTORY_INDEX_PREFIX = 'daily_'
TMP_FILES_DIRECTORY = '/../tmp/'

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
                }
            }
        }
        if es.indices.exists(index=HISTORY_INDEX_PREFIX + coin):
            es.indices.delete(index=HISTORY_INDEX_PREFIX + coin)
        es.indices.create(index=HISTORY_INDEX_PREFIX + coin, mappings=requestBody)

    parseData(es)
def parseData(esConnection):
    coinsList = [os.environ['COINMARKETCAP_BTC_ID'], os.environ['COINMARKETCAP_ETH_ID'], os.environ['COINMARKETCAP_BNB_ID']]
    yearsList = range(2017, 2025, 1) # 1.1.2017 - 1.1.2025

    for progressBar in tqdm([[{'coin': coin, 'year': y} for y in yearsList] for coin in coinsList], desc="Parse data..."):
        for iteration in progressBar:
            coin = iteration['coin']
            year = iteration['year']
            #print(coin, year)

            startDate = int(round(datetime.datetime(year, 1, 1, 0).timestamp()))
            endDate = int(round(datetime.datetime(year + 1, 1, 1, 0).timestamp()))

            content = ""
            if (os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + TMP_FILES_DIRECTORY + str(coin) + "_" + str(year) + '.json')):
                with open(os.path.dirname(os.path.realpath(__file__)) + TMP_FILES_DIRECTORY + str(coin) + "_" + str(year) + '.json') as file:
                    content = json.load(file)
                    file.close()
            else:
                url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical?id="+str(coin)+"&convertId=2781&timeStart="+str(startDate)+"&timeEnd="+str(endDate)
                content = json.loads(requests.get(url).content)
                with open(os.path.dirname(os.path.realpath(__file__)) + TMP_FILES_DIRECTORY + str(coin) + "_" + str(year) + '.json', 'w') as file:
                    file.write(json.dumps(content))
                    file.close()

            if (content['status']['error_message'] == "SUCCESS"):
                indexName = HISTORY_INDEX_PREFIX + content['data']['symbol'].lower()
                for archivePrice in content['data']['quotes']:
                    doc = convertCoinMarketObject(archivePrice)
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

# if __name__ == "__main__":
migrate()