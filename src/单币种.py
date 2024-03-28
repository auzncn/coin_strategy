import ccxt
import pandas as pd
import numpy as np

# 初始化币安交易所
exchange = ccxt.binance({
    'proxies': {
        'http': 'http://localhost:1080',
        'https': 'http://localhost:1080',
    },
})

data = exchange.fetch_ohlcv("BTC/USDT", '4h',limit=1500)
df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
# df.set_index('timestamp', inplace=True)

            # 计算ema12, ema144, ema169
df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
df['ema144'] = df['close'].ewm(span=144, adjust=False).mean()            
df['ema169'] = df['close'].ewm(span=169, adjust=False).mean()            
df['ema576'] = df['close'].ewm(span=576, adjust=False).mean()            
df['ema676'] = df['close'].ewm(span=676, adjust=False).mean()

# 计算5日移动平均价和10日移动平均量
df['ma5'] = df['close'].rolling(window=6).mean()
df['ma10'] = df['volume'].rolling(window=12).mean()

# 将ma5和ma10往后移动7个时段，表示7个时段后的情况
df['shifted_ma5'] = df['ma5'].shift(-6)
df['shifted_ma10'] = df['ma10'].shift(-6)

# 判断量价是否出现背离
df['divergence'] = (df['close'] > df['shifted_ma5']) & (df['volume'] < df['shifted_ma10'])
print(df.tail(50))

# 输出背离数据
# print(df.loc[df['divergence']].tail(50))


# df['ma12'] = df['close'].rolling(window=12).mean()
# df['ma144'] = df['close'].rolling(window=144).mean()       
# df['ma169'] = df['close'].rolling(window=169).mean()           
# df['ma576'] = df['close'].rolling(window=576).mean()          
# df['ma676'] = df['close'].rolling(window=676).mean()
# print(df)