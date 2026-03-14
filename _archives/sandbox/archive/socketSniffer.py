import json
import websocket
import datetime

"""Демонстрация работы получения цен пары ETH-USDT по сокету"""
def ws_trades():
    """Подключение"""
    socket = f'wss://stream.binance.com:9443/ws/ethusdt@trade'

    def on_message(wsapp, message):
        json_message = json.loads(message)
        handle_trades(json_message)

    def on_error(wsapp, error):
        print(error)

    # def on_open(ws):
    #     print('opened connection')
    #
    # def on_close(ws):
    #     print('close connection')
    #
    # def on_message(ws, message):
    #     print('received message')
    #     print(message)
    #
    # def on_error(ws, message):
    #     print('error:', message)
    #
    # ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message,
    #                             on_error=on_error)
    # ws.run_forever()

    wsapp = websocket.WebSocketApp(socket, on_message=on_message, on_error=on_error)
    wsapp.run_forever()


def handle_trades(json_message):
    """
    Вывод параметров
    json_message - словарь JSON
    """
    date_time = datetime.datetime.fromtimestamp(json_message['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    print("SYMBOL: " + json_message['s'])
    print("PRICE: " + json_message['p'])
    print("QTY: " + json_message['q'])
    print("TIMESTAMP: " + str(date_time))
    print("-----------------------")

#ws_trades()
#
# """Демонстрация работы получения цен пары ETH-USDT по сокету"""
#
# def wsTradesNew():
#     """Подключение"""
#     socket = f'wss://ws-api.binance.com:443/ws-api/v3'
#
#     def on_message(wsapp, message):
#         json_message = json.loads(message)
#         print(json_message)
#         #handle_trades(json_message)
#
#     def on_error(wsapp, error):
#         print(error)
#
#     request =  '''
#         {
#           "id": "e2a85d9f-07a5-4f94-8d5f-789dc3deb097",
#           "method": "order.place",
#           "params": {
#             "symbol": "BTCUSDT",
#             "side": "BUY",
#             "type": "LIMIT",
#             "price": "0.1",
#             "quantity": "10",
#             "timeInForce": "GTC",
#             "timestamp": 1655716096498,
#             "apiKey": "T59MTDLWlpRW16JVeZ2Nju5A5C98WkMm8CSzWC4oqynUlTm1zXOxyauT8LmwXEv9",
#             "signature": "5942ad337e6779f2f4c62cd1c26dba71c91514400a24990a3e7f5edec9323f90"
#           }
#         }
#         '''
#     wsapp = websocket.WebSocketApp(socket, header= request, on_message=on_message, on_error=on_error)
#     wsapp.run_forever()
#
# wsTradesNew()