import os
import sys
import orjson
from elasticsearch import Elasticsearch

HISTORY_INDEX_PREFIX = 'daily_' if len(sys.argv) == 1 else 'daily_' + sys.argv[1]
print(HISTORY_INDEX_PREFIX)

# print((os.path.dirname(os.path.realpath(__file__)) + "/../" + os.environ["TRADE_PAIRS_CONFIG"]))
# configs/tradePairs/example.json ~ first
# if (os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + "/../" + os.environ["TRADE_PAIRS_CONFIG"])):
#     with open(os.path.dirname(os.path.realpath(__file__)) + "/../" + os.environ["TRADE_PAIRS_CONFIG"]) as file:
#         fileContent = file.read()
#         t = orjson.loads(fileContent)
#         file.close()
# if (os.path.isfile(os.path.dirname(os.path.realpath(__file__)) + "/../" + os.environ["EXCHANGE_COMMON_PARAMETERS"])):
#     with open(os.path.dirname(os.path.realpath(__file__)) + "/../" + os.environ["EXCHANGE_COMMON_PARAMETERS"]) as file:
#         fileContent = file.read()
#         e = orjson.loads(fileContent)
#         file.close()

# print(e['feeType'])
# print(t['TRADING_STRATEGY_ETHUSDT']['COIN_A'])


# todo если вчера падал, то сегодня ... (недельный тренд + вчера)


es = Elasticsearch(os.environ['ELASTICSEARCH_HOST'])

index = "daily_eth"

data = es.search(index=index, query={"match_all": {}}, size=10000, sort='timeOpen')
data = data['hits']['hits'][7:]
data = data[400:1500]
#data = data[-:400] # 4 квартал 2022
#data = data[1750:] # с конца 2021 (нисходящий тренд)

currentOrderType = "BUY"
currentOrderPrice = 0.
coinValue = 0.
deposit = 1000
fee = 0.001
feeSum = 0
profit = 0.
waitMode = False
closedAll = 0
wait = 0
waitAll = 0
basta = 10000
profitPercent = 0.03

def makeOrder(price):
    global currentOrderPrice
    global currentOrderType
    global waitMode
    currentOrderPrice = price
    print("(" + currentOrderType + ") Ставлю ордер по " + str(price))
    waitMode = True
def checkOrder(day):
    global currentOrderPrice
    global currentOrderType
    global coinValue
    global deposit
    global closedAll
    global wait
    global waitAll
    global waitMode
    global fee
    global feeSum
    global basta

    if currentOrderType == "BUY":
        if day['openPrice'] <= currentOrderPrice or day['highPrice'] <= currentOrderPrice \
                or day['lowPrice'] <= currentOrderPrice or day['closePrice'] <= currentOrderPrice:
            coinValue = coinValue + (deposit / currentOrderPrice)
            feeSum += deposit * fee
            deposit = deposit - coinValue * currentOrderPrice
            wait = 0
            closedAll += 1
            waitMode = False
            print("----- Купил по " + str(currentOrderPrice) + ". Баланс " + str(coinValue) + " ETH, $$$ = " + str(deposit))
            return True
        else:
            wait +=1
            waitAll +=1

            if wait > basta:
                print("===== Устал ждать, покупаю по  " + str(day['closePrice']))
                coinValue = coinValue + (deposit / day['closePrice'])
                feeSum += deposit * fee
                deposit = deposit - coinValue * day['closePrice']
                wait = 0
                closedAll += 1
                waitMode = False
                return True


    if currentOrderType == "SELL":
        if day['openPrice'] >= currentOrderPrice or day['highPrice'] >= currentOrderPrice \
                or day['lowPrice'] >= currentOrderPrice or day['closePrice'] >= currentOrderPrice:
            deposit = deposit + coinValue * currentOrderPrice
            coinValue = 0
            feeSum += coinValue * currentOrderPrice * fee

            wait = 0
            closedAll += 1
            waitMode = False
            print("----- Продал по " + str(currentOrderPrice) + ". Баланс " + str(coinValue) + " ETH, $$$ = " + str(deposit))
            return True
        else:
            wait += 1
            waitAll += 1

            if wait > basta:
                print("===== Устал ждать, продаю по  " + str(day['closePrice']))
                deposit = deposit + coinValue * day['closePrice']
                coinValue = 0
                feeSum += coinValue * day['closePrice'] * fee
                wait = 0
                closedAll += 1
                waitMode = False
                return True

    return False

for day in data:
    source = day['_source']
    print("##### Сегодня " + source['timeOpen'] + ". У меня " + str(deposit) + "$$$")
    if waitMode == False:
        if currentOrderType == "BUY":
            makeOrder((source['openPrice'] if closedAll == 0 else source['closePrice']) * (1 - profitPercent))
        elif currentOrderType == "SELL":
            makeOrder(source['openPrice'] * (1 + profitPercent))

        print("===== Жду закрытия ордера " + currentOrderType + " уже " + str(wait) + "дней")
        if checkOrder(source):
            currentOrderType = "BUY" if currentOrderType == "SELL" else "SELL"
    else:
        print("===== Жду закрытия ордера " + currentOrderType + " уже " + str(wait) + "дней")
        if checkOrder(source):
            currentOrderType = "BUY" if currentOrderType == "SELL" else "SELL"


print("feeSum: " + str(feeSum))
print("closedAll: " + str(closedAll))
print("waitAll: " + str(closedAll))

print("Итого у меня к " + source['timeOpen'] + ".  " + str(deposit) + " $$$ и " + str(coinValue) + " ETH ")
print("Депозит эквивалентен " + str(deposit if deposit > 0 else coinValue * source['openPrice']) + " $$$")

# todo: реализовать продажу при падении на 1%, либо ожидание при росте
