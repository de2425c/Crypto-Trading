import asyncio
import json
import os
from datetime import datetime
import pytz
from websockets import connect

trades_filename = 'recent_trades.csv'
funding_filename = 'funding_rates.csv'
liquidations_filename = 'liquidation_events.csv'


for filename, headers in [
    (trades_filename, 'Event Time, Symbol, Trade ID, Price, Quantity, Trade Time, Is Buyer Maker\n'),
    (funding_filename, 'Timestamp, Symbol, Funding Rate, Yearly Rate\n'),
    (liquidations_filename, 'Symbol, Side, Quantity, Price, USD Size, Event Time\n')
]:
    if not os.path.isfile(filename):
        with open(filename, 'w') as f:
            f.write(headers)


async def track_trades(uri, filename):
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                usd_size = float(data['p']) * float(data['q'])
                if usd_size > 15000:
                    trade_time = datetime.fromtimestamp(data['T'] / 1000, pytz.utc)
                    readable_trade_time = trade_time.strftime('%Y-%m-%d %H:%M:%S')
                    with open(filename, 'a') as f:
                        f.write(f"{data['E']}, {data['s']}, {data['a']}, {data['p']}, {data['q']}, {readable_trade_time}, {data['m']}\n")

            except Exception as e:
                print(f"Error in track_trades: {e}")
                await asyncio.sleep(5)

async def track_funding_rates(symbol, filename):
    uri = f"wss://fstream.binance.com/ws/{symbol}@markPrice"
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                timestamp = datetime.utcfromtimestamp(data['E'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                funding_rate = float(data['r'])
                yearly_rate = (funding_rate * 3 * 365) * 100

                with open(filename, 'a') as f:
                    f.write(f"{timestamp}, {data['s']}, {funding_rate}, {yearly_rate}\n")

            except Exception as e:
                print(f"Error in track_funding_rates: {e}")
                await asyncio.sleep(5)

async def track_liquidations(uri, filename):
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)['o']
                symbol = data['s']
                side = data['S']
                quantity = float(data['z'])
                price = float(data['p'])
                usd_size = quantity * price
                timestamp = datetime.utcfromtimestamp(data['T'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

                if usd_size > 3000:
                    with open(filename, 'a') as f:
                        f.write(f"{symbol}, {side}, {quantity}, {price}, {usd_size}, {timestamp}\n")

            except Exception as e:
                print(f"Error in track_liquidations: {e}")
                await asyncio.sleep(5)

 
async def main():
    symbols = ['btcusdt', 'solusdt', 'ethusdt']

    trade_tasks = [track_trades(f"wss://fstream.binance.com/ws/{symbol}@aggTrade", trades_filename) for symbol in symbols]
    funding_tasks = [track_funding_rates(symbol, funding_filename) for symbol in symbols]
    liquidation_task = track_liquidations("wss://fstream.binance.com/ws/!forceOrder@arr", liquidations_filename)
    await asyncio.gather(*trade_tasks, *funding_tasks, liquidation_task)

asyncio.run(main())


