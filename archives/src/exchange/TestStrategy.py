from .exchangeAbstractClass import exchangeAbstractClass
from elasticsearch import Elasticsearch
from datetime import datetime
import os


class TestStrategy(exchangeAbstractClass):
    marketSequence = None
    marketSequenceGenerator = None

    currentPriceKey = 'TEST_CURRENT_PRICE'
    currentOrderTypeKey = 'TEST_ORDER_TYPE'
    currentOrderPriceKey = 'TEST_ORDER_PRICE'
    currentOrderAmountKey = 'TEST_ORDER_AMOUNT'
    currentOrderIdKey = 'TEST_ORDER_ID'
    balanceKeyPrefix = 'TEST_BALANCE_'
    feeSumKey = 'TEST_FEE_SUM'
    waitedTicks = 'TEST_WAITED'

    def connect(self):
        self.sendMessage('INFO', 'Test Exchange has chosen', {})
        self.session = Elasticsearch(os.environ['ELASTICSEARCH_HOST'])

        self.cache.delete(self.currentOrderIdKey)
        self.cache.setKeyValue(self.feeSumKey, 0)

        # sorted history of market
        index = "daily_" + self.config['COIN'].lower()
        self.marketSequence = self.session.search(index=index, query={"match_all": {}}, size=10000, sort='timeOpen')
        self.marketSequence = self.marketSequence['hits']['hits'][7:]

        self.sendMessage(
                    'DEBUG',
                         'Market sequence from ' +
                         self.marketSequence[self.config['START_SEQUENCE_DAY']]['_id'] +
                         ' to ' + self.marketSequence[self.config['END_SEQUENCE_DAY']]['_id'],
                    {
                        'START_PRICE': self.marketSequence[self.config['START_SEQUENCE_DAY']]['_source']['openPrice'],
                        'END_PRICE': self.marketSequence[self.config['END_SEQUENCE_DAY']]['_source']['openPrice']
                    })

        self.marketSequence = self.marketSequence[self.config['START_SEQUENCE_DAY']:self.config['END_SEQUENCE_DAY']]
        self.marketSequenceGenerator = self.getPriceGenerator()

    def getBalance(self):
        return {
            'COIN': float(self.cache.getKeyValue(self.balanceKeyPrefix + self.config['COIN'])),
            'STABLECOIN': float(self.cache.getKeyValue(self.balanceKeyPrefix + self.config['STABLECOIN']))
        }

    def setBalance(self, ticker: str, amount: float):
        self.cache.setKeyValue(self.balanceKeyPrefix + ticker, float(amount))

    def placeOrder(self, type: str, price: float, amountPercent: float):
        self.cache.setKeyValue(self.currentOrderPriceKey, price)
        self.cache.setKeyValue(self.currentOrderTypeKey, type)
        # todo: always 100%
        #self.cache.setKeyValue(self.currentOrderAmountKey, amountPercent)

        orderId = self.cache.getKeyValue(self.currentOrderIdKey)
        orderId = 1 if orderId is None else int(orderId) + 1
        self.cache.setKeyValue(self.currentOrderIdKey, orderId)

        self.sendMessage(
            'OK',
            'Order ' + str(orderId) + ' has placed',
            {
                    'ORDER_ID': orderId,
                    'ORDER_TYPE': type,
                    'ORDER_PRICE': price,
                    'AMOUNT_PERCENT': amountPercent
            })

        # if orderId == 2:
        #     exit()

        return orderId

    def getMarketData(self, symbol):
        try: 
            marketData = next(self.marketSequenceGenerator)
            self.cache.setKeyValue(self.currentPriceKey, float(marketData['price']))

            waitedTicks = self.cache.getKeyValue(self.waitedTicks)
            waitedTicks = 1 if waitedTicks is None else int(waitedTicks) + 1
            self.cache.setKeyValue(self.waitedTicks, waitedTicks)

            return marketData
        except StopIteration:
            return None
        finally:
            # emulate websocket event
            self.checkOrder(self.cache.getKeyValue(self.currentOrderIdKey))

    # ordered by time price generator
    def getPriceGenerator(self):
        for currentDay in self.marketSequence:
            currentDay = currentDay['_source']

            timeHigh = datetime.fromisoformat(currentDay['timeHigh'])
            timeLow = datetime.fromisoformat(currentDay['timeLow'])
            timeAvg = str((max(timeHigh, timeLow) - min(timeHigh, timeLow)) / 2 + min(timeHigh, timeLow))

            currentDayPrices = {
                currentDay['timeOpen']: currentDay['openPrice'],
                currentDay['timeHigh']: currentDay['highPrice'],
                timeAvg: currentDay['avgPrice'],
                currentDay['timeLow']: currentDay['lowPrice'],
                currentDay['timeClose']: currentDay['closePrice'],
            }

            currentDayPrices = dict(sorted(currentDayPrices.items(), key = lambda x:x[0]))

            for time, price in currentDayPrices.items():
                yield {'price': price, 'time': time}
                next(iter(currentDayPrices))

    def cancelOrder(self, orderId):
        pass

    def checkOrder(self, orderId):
        if orderId is None:
            return None
        else:
            orderId = int(orderId)

        currentOrderPrice = float(self.cache.getKeyValue(self.currentOrderPriceKey))
        currentOrderType = str(self.cache.getKeyValue(self.currentOrderTypeKey))
        currentPrice = float(self.cache.getKeyValue(self.currentPriceKey))
        #amountPercent = float(self.cache.getKeyValue(self.currentOrderAmountKey))
        coinAmount = float(self.cache.getKeyValue(self.balanceKeyPrefix + self.config['COIN']))
        deposit = float(self.cache.getKeyValue(self.balanceKeyPrefix + self.config['STABLECOIN']))
        feeSum = self.cache.getKeyValue(self.feeSumKey)
        feeSum = 0 if feeSum is None else float(feeSum)
        feeCurrent = 0
        fee = float(self.config['FEE'])
        waited = int(self.cache.getKeyValue(self.waitedTicks))

        isComplete = False

        self.sendMessage(
            'DEBUG',
            'checkOrder ' + str(orderId),
            {
                'ORDER_ID': orderId,
                'ORDER_TYPE': currentOrderType,
                'ORDER_PRICE': currentOrderPrice,
                'CURRENT_PRICE': currentPrice,
                'CONDITION': (currentPrice <= currentOrderPrice) if currentOrderType == "BUY" else (currentPrice >= currentOrderPrice),
                'WAITED': waited,
                'STABLECOIN_AMOUNT': deposit,
                'COIN_AMOUNT': coinAmount,
                'FEE_SUM': feeSum
            })

        if currentOrderType == "BUY":
            if currentPrice <= currentOrderPrice and self.checkCandle(deposit):
                deposit = deposit
                coinAmount = (deposit / currentOrderPrice)
                # imitation of work with the current price (bot transmits it)
                #coinAmount = (deposit / currentPrice)
                #currentOrderPrice = currentPrice

                feeCurrent = deposit * fee
                feeSum += feeCurrent
                deposit = 0 # deposit - coinAmount * currentOrderPrice
                isComplete = True

                self.sendMessage('OK', 'Buy-order completed, price = ' + str(currentOrderPrice) + ', balance: ' +
                                 str(deposit) + ' $,' + str(coinAmount) + ' coin', {})
        elif currentOrderType == "SELL":
            if currentPrice >= currentOrderPrice and self.checkCandle(deposit):
                #deposit = deposit + coinAmount * currentOrderPrice
                # imitation of work with the current price (bot transmits it)
                #deposit = coinAmount * currentPrice
                deposit = coinAmount * currentOrderPrice
                #currentOrderPrice = currentPrice

                feeCurrent = deposit * fee
                #feeSum += coinAmount * currentOrderPrice * fee
                feeSum += deposit * fee
                coinAmount = 0

                self.sendMessage('OK', 'Sell-order completed, price = ' + str(currentOrderPrice) + ', balance: ' +
                                 str(deposit) + ' $,' + str(coinAmount) + ' coin', {})
                isComplete = True

        # create event, refresh cache
        if isComplete:
            self.cache.setKeyValue(self.currentOrderPriceKey, currentOrderPrice)
            self.cache.setKeyValue(self.currentOrderTypeKey, "BUY" if currentOrderType == "SELL" else "SELL")
            self.cache.setKeyValue(self.balanceKeyPrefix + self.config['COIN'], coinAmount)
            self.cache.setKeyValue(self.balanceKeyPrefix + self.config['STABLECOIN'], deposit)
            self.cache.setKeyValue(self.feeSumKey, feeSum)

            # reset counter
            self.cache.setKeyValue(self.waitedTicks, 0)

            self.cache.orderEvent(
                {
                    'ORDER_ID': orderId,
                    'ORDER_TYPE': currentOrderType,
                    'ORDER_PRICE': currentOrderPrice,
                    'COIN_AMOUNT': coinAmount,
                    'DEPOSIT': deposit,
                    'FEE': feeCurrent,
                    'FEE_SUM': feeSum
                }
            )

        return None

    def checkCandle(self, currentDeposit: float) -> bool:
        # always enough
        return True
