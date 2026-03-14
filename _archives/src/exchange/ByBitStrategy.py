from .exchangeAbstractClass import exchangeAbstractClass
#from pybit.unified_trading import HTTP
from pybit.unified_trading import WebSocket
import os

class ByBitStrategy(exchangeAbstractClass):
    def connect(self):
        self.sendMessage('INFO', 'ByBitStrategy has chosen', {})
        # self.session = HTTP(
        #     testnet=os.environ['TEST_NET'],
        #     api_key=os.environ['API_KEY'],
        #     api_secret=os.environ['SECRET_KEY'],
        # )
        self.session = WebSocket(
            testnet=os.environ['TEST_NET'],
            channel_type="private",
            api_key=os.environ['API_KEY'],
            api_secret=os.environ['SECRET_KEY'],
        )

        # def handle_message(message):
        #     print(message)
        # https://bybit-exchange.github.io/docs/v5/websocket/private/order
        # ws.order_stream(callback=handle_message)

    def getBalance(self):
        # print(session.get_orderbook(category="linear", symbol="BTCUSDT"))
        balanceAnswer = self.session.get_wallet_balance(
            accountType="UNIFIED",
            coin="USDT,ETH"
        )
        #print(balanceAnswer)
        # list
        # {
        #     "availableToBorrow": "",
        #     "bonus": "0",
        #     "accruedInterest": "0",
        #     "availableToWithdraw": "0.0355235",
        #     "totalOrderIM": "0",
        #     "equity": "0.0355235",
        #     "totalPositionMM": "0",
        #     "usdValue": "0.03553223",
        #     "unrealisedPnl": "0",
        #     "collateralSwitch": true,
        #     "spotHedgingQty": "0",
        #     "borrowAmount": "0.000000000000000000",
        #     "totalPositionIM": "0",
        #     "walletBalance": "0.0355235",
        #     "cumRealisedPnl": "-8.435952",
        #     "locked": "0",
        #     "marginCollateral": true,
        #     "coin": "USDT"
        # }

        for coinBalance in balanceAnswer['result']['list'][0]['coin']:
            print(coinBalance['walletBalance'])
            print(coinBalance['coin'])
            # BALANCE_
            self.cache.setKeyValue(coinBalance['coin'], coinBalance['walletBalance'])

    def placeOrder(self, symbol, quantity, orderType, price=None):
        print(self.session.place_order(
            category="linear",
            symbol="BTCUSDT",
            side="Buy",
            orderType="Market",
            qty=0.001,
        ))

    def getMarketData(self, symbol):
        pass

    def cancelOrder(self, orderId):
        pass

    def checkOrder(self, orderId):
        pass