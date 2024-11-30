import pandas as pd

file_path = 'Bitstamp_BTCUSD_1h.csv'
data = pd.read_csv(file_path)

data.rename(
    columns={
        'unix': 'timestamp',
        'date': 'datetime',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'Volume BTC': 'VolumeBTC',
        'Volume USD': 'VolumeUSD'
    },
    inplace=True
)

newfile = 'backtrader_' + file_path 
data[['datetime', 'Open', 'High', 'Low', 'Close', 'VolumeBTC']].to_csv(
    newfile, index=False
)