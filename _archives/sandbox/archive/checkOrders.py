import json
import os
import sys
import websocket
from threading import Thread
from queue import Queue
import hmac
import time
import hashlib
import requests

hostURL = os.environ['HOST_URL']
trade_step = os.environ.get('TRADE_STEP') if os.environ.get('TRADE_STEP') else 30 #60
api_key, api_secret,  = os.environ['API_KEY'], os.environ['SECRET_KEY'],
balance, openOrders, limits = None, None, None

def printBalance():
    print('Баланс:')
    for key, val in balance.items():
        print(f'{key}: {val}')

def tryCloseOpenOrder(orderId:int):
    url = f'https://{hostURL}/api/v3/order'
    params = {
        'symbol': 'ETHUSDT',
        'orderId': orderId,
        'timestamp': get_timestamp(),
    }
    r_signature = makeString(params)
    params['signature'] = hashing(r_signature)
    response = requests.delete(url, params=params, headers={'X-MBX-APIKEY': api_key})
    if response:
        response_json = response.json()
        print(f"{response_json['orderId']}: {response_json['status']}")
    else:
        print('Ошибка в ходе получения ответа на отмену ордера')
        sys.exit()
def printOpenOrders():
    if openOrders:
        print('\nID открытых сделок:')
        for item in openOrders:
            print(f'{item}, ', end=' ')
    else:
        print('Открытых сделок нет')

def hashing(query):
    return hmac.new(
        api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()

def get_timestamp():
    return int(time.time() * 1000)

priceq = Queue()
pricelist = []

def makeString(dictionary, ordered = False):
    arr = [(k, v) for (k, v) in dictionary.items()]
    if ordered:
        arr = sorted(dictionary.items(), key = lambda x:x[0])
    res = str(arr[0][0])+'='+str(arr[0][1])
    for i, elem in enumerate(arr):
        if i == 0:
            continue
        res+='&'+str(elem[0])+'='+str(elem[1])
    return res

def updateOpenOrdersInfo():
    global openOrders
    url = f'https://{hostURL}/api/v3/openOrders'
    params = {
        'timestamp': get_timestamp()
    }
    r_signature = makeString(params)
    params['signature'] = hashing(r_signature)
    response = requests.get(url, params = params, headers={'X-MBX-APIKEY': api_key})
    if response.status_code != 200:
        return False
    resp_json = response.json()
    openOrders = [item["orderId"] for item in resp_json] #Даже не знаю, что делать с этой инфой

def getLimitsInfo():
    url = f'https://{hostURL}/api/v3/exchangeInfo'
    response = requests.get(url, headers={'X-MBX-APIKEY': api_key})
    response_json = response.json()
    limits = response_json.get('rateLimits')
    print(f'\n Limits:{limits}')

def updateBalanceInfo():
    global balance
    url = f'https://{hostURL}/api/v3/account'
    params = {
        'timestamp': get_timestamp()
    }
    r_signature = makeString(params, ordered=False)
    params['signature'] = hashing(r_signature)
    response = requests.get(url, params=params, headers={'X-MBX-APIKEY': api_key})
    if response.status_code != 200:
        return False
    # Ответ содержит больше полей, например, комиссии, но выводить на экран буду только баланс, так как те же комиссии нулевые
    #print(response.text)
    resp_json = response.json()['balances']
    balance = {item['asset']: float(item['free']) for item in resp_json if (item['asset'] in {'ETH', 'USDT'})}
    #Если торговать сразу на нескольких парах, надо написать метод чуть-чуть умнее

def sellOrBuy(side:str, quantity:float = None, quoteOrderQty:float = None, price:float = None, ordertype:str = 'MARKET', tif:str = 'GTC'):
    #side = 'sell'
    #ordertype = 'LIMIT'
    #price = 1600.670000
    url = f'https://{hostURL}/api/v3/order'
    params = {
        'symbol': 'ETHUSDT',
        'side': side,
        'type': ordertype,
        'timestamp': get_timestamp(),
    }
    if(ordertype == 'LIMIT'): #some flexibility
        params['price'] = str(price)
        params['timeInForce'] = tif  #todo поиграться с параметром
    if quantity:
        params['quantity'] = str(quantity)
    if quoteOrderQty:
        params['quoteOrderQty'] = str(quoteOrderQty)
    r_signature = makeString(params)
    params['signature'] = hashing(r_signature)
    response = requests.post(url, params = params, headers={'X-MBX-APIKEY': api_key})
    print(f'\nОтвет на размещение ордера:\n{response.text} {price}')
    if response:
        response_json = response.json()
        #status = response_json.get('status')
        #print(status) для MARKET это всегда (?) FILLED
        #записываем состояния сделок в openOrders
        #print(response.text)
    else:
        print('Ошибка в ходе получения ответа на размещение ордера')
        sys.exit()

counter = 1
def makeOrderAndWait(): # Здесь осуществляется проверка изменения цен на соответствие выигрышным сценарием и сделка
    global counter
    if (counter % 7 == 0):
        sellOrBuy(side = "sell", quoteOrderQty= 15)
    counter += 1

def pricelistTrim():
    global pricelist
    if len(pricelist) > 1000:
        pricelist = pricelist[1:]
    #todo Таккже нужно реализовать проверку по времени

def orders():
    global priceq
    global pricelist
    while True:
        while priceq.qsize():
            pricetuple = priceq.get()
            makeOrderAndWait() #Проверка изменения цен на наличие паттернов и совершение сделки
            pricelist.append(pricetuple) #
            pricelistTrim() #Стек с ценами обрезается

def init_listen():
    socket = f'wss://{hostURL}/ws/ethusdt@trade'
    def on_open(wsapp):
        print('Listening to price updates has started')
    def on_message(wsapp, message):
        json_message = json.loads(message)
        handle_trades(json_message) #Отправляем сущность цена на обработку
    def on_error(wsapp, error):
        print(f'Listening to price updates has closed: {error}')
        sys.exit()
    def on_close(wsapp):
        print('Listening to price updates has closed')
        sys.exit()
    wsapp = websocket.WebSocketApp(socket, on_open = on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    wsapp.run_forever()
torders = Thread(target = orders)

working = False
def orders_status():
    pingurl = f'https://{hostURL}/api/v3/userDataStream' #
    response = requests.post(pingurl, headers={'X-MBX-APIKEY': api_key})
    #print(response.text)
    response_json = response.json()
    userdata_listenkey = response_json.get('listenKey')
    if not userdata_listenkey:
        print('Failed to obtain listenKey for account updates')
        sys.exit()
    key_fresh = 1650
    listenupdates_socket = f'wss://{hostURL}/ws/{userdata_listenkey}'
    def ping_loop():
            while True:
                response = requests.put(f'https://{hostURL}/fapi/v1/{userdata_listenkey}', headers={'X-MBX-APIKEY': api_key})
                print(f'Ping request sent') #{response.text}')
                sys.stdout.flush()
                time.sleep(key_fresh)
    tping_loop = Thread(target=ping_loop)
    def on_message(wsapp, message):
        #print(f'Account update from web-socket:\n{message}')
        response_json = json.loads(message)
        if response_json.get('code'):
            print(f'Ошибка: {message}')
            sys.exit()
        if response_json.get('e') == 'outboundAccountPosition':
            for item in response_json['B']:
                balance[item['a']] = float(item['f']) #записываем обновление баланса
            printBalance()
        if response_json['e'] == 'executionReport':
            print(message)
    def on_error(wsapp, error):
        print(error)
        print('Listening to order updates has closed')
        sys.exit()
    def on_open(wsapp):
        print('Listening to order updates has started')
        tping_loop.start()
    def on_close(wsapp):
        print('Listening to order updates has closed')
        sys.exit()
    wsapp = websocket.WebSocketApp(listenupdates_socket, on_message=on_message, on_error=on_error, on_open=on_open, on_close=on_close)
    wsapp.run_forever()
torders_status = Thread(target = orders_status)
def handle_trades(json_message):
    global priceq
    global working
    price = float(json_message['p'])
    price_time = float(json_message['t'])
    priceq.put((price, price_time))
    sys.stdout.flush()
    if not working:
        working = True
        torders.start()
        torders_status.start()

updateBalanceInfo()
updateOpenOrdersInfo()
printBalance()
printOpenOrders()
'''
for _order in openOrders:
    tryCloseOpenOrder(_order)
    time.sleep(0.3)
'''
getLimitsInfo()
init_listen()

#