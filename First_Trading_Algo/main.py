import backtrader as bt
import pandas as pd
import datetime

backtrader_file = 'backtrader_Bitstamp_BTCUSD_1h.csv'

#SMA Bot: takes in "strategy param"
class SMABot(bt.Strategy):
    params = {'sma_period':20} #20 day SMA
    
    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.sma_period)
        
    def next(self):
        if not self.position:
            if self.data.close[0] > self.sma[0]:
                self.buy(size = 0.1)
        else:
            if self.data.close[0] < self.sma[0]:
                self.sell(size=0.1)
                
cerebro = bt.Cerebro()

data_feed = bt.feeds.GenericCSVData(
    dataname=backtrader_file,
    dtformat='%Y-%m-%d %H:%M:%S',
    timeframe=bt.TimeFrame.Minutes,
    compression=60,
    openinterest=-1,
    datetime=0, open=1, high=2, low=3, close=4, volume=5
)

cerebro.adddata(data_feed)
cerebro.addstrategy(SMABot)

cerebro.broker.setcash(100000)
cerebro.broker.setcommission(commission=0.001) #0.1% commission

print(f'Starting Portfolio Value: {cerebro.broker.getvalue()}')
cerebro.run()
print(f'Final Portfolio Value: {cerebro.broker.getvalue()}')
cerebro.plot()