import ccxt
import pandas as pd
import datetime
import time
import requests
import json
from retry import retry


# 初始化币安交易所对象
exchange = ccxt.binance({
    'proxies': {
        'http': 'http://localhost:7890',
        'https': 'http://localhost:7890',
    },
})

markets = exchange.load_markets()
usdt_pairs = [symbol for symbol, info in markets.items(
) if 'USDT' in symbol and info.get('spot', True) and info.get('active', True)]


def calculate_macd(src, fast_length, slow_length, signal_length, sma_source, sma_signal):
    fast_ma = src.rolling(window=fast_length).mean(
    ) if sma_source == "SMA" else src.ewm(span=fast_length).mean()
    slow_ma = src.rolling(window=slow_length).mean(
    ) if sma_source == "SMA" else src.ewm(span=slow_length).mean()
    macd = fast_ma - slow_ma
    signal = macd.rolling(window=signal_length).mean(
    ) if sma_signal == "SMA" else macd.ewm(span=signal_length).mean()
    hist = macd - signal
    return hist


def send_message(msg):

    headers = {'content-type': 'application/json'}
    data = {'msgtype': 'text', 'text': {'content': msg}}
    requests.post(url='',
                  headers=headers, data=json.dumps(data))


@retry(tries=100, delay=5)
def job():
    for symbol in usdt_pairs:
        print("开始获取"+symbol)
        time.sleep(exchange.rateLimit / 1000)
        timeframe = '4h'
        ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, limit=400)

        # 将 OHLCV 数据转换为 DataFrame
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(ohlcvs, columns=columns)
        df['timestamp'] = pd.to_datetime(
            df['timestamp'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
        df.set_index('timestamp', inplace=True)

        # 计算指数移动平均线
        fast_ema_period = 12
        slow_ema_period = 25
        def_ema_period = 25
        ema_200 = 200

        df['fast_ema'] = df['close'].ewm(span=fast_ema_period).mean()
        df['slow_ema'] = df['close'].ewm(span=slow_ema_period).mean()
        df['bias_ema'] = df['close'].ewm(span=def_ema_period).mean()
        df['ema_200'] = df['close'].ewm(span=ema_200).mean()

        # 计算买卖信号
        df['buy_signal'] = (df['fast_ema'] > df['slow_ema']) & (
            df['fast_ema'].shift(1) <= df['slow_ema'].shift(1))
        df['sell_signal'] = (df['fast_ema'] < df['slow_ema']) & (
            df['fast_ema'].shift(1) >= df['slow_ema'].shift(1))

        df['up_on_ema200'] = (df['close'] > df['ema_200'])
        macd_hist = calculate_macd(df['close'], 26, 100, 9, "EMA", "EMA")
        df['macd_hist'] = macd_hist

        # 计算连续出现的买卖信号数量
        df['count_buy'] = 0
        df['count_sell'] = 0

        for i in range(1, len(df)):
            if df.at[df.index[i], 'buy_signal']:
                df.at[df.index[i], 'count_buy'] = df.at[df.index[i-1],
                                                        'count_buy'] + 1
                df.at[df.index[i], 'count_sell'] = 0
            elif df.at[df.index[i], 'sell_signal']:
                df.at[df.index[i], 'count_sell'] = df.at[df.index[i-1],
                                                         'count_sell'] + 1
                df.at[df.index[i], 'count_buy'] = 0

        # 计算买入和卖出信号
        df['buysignal'] = (df['count_buy'] < 2) & (df['count_buy'] > 0) & (
            df['count_sell'] < 1) & df['buy_signal'] & (~df['buy_signal'].shift(1).fillna(False)).astype(int)
        df['sellsignal'] = (df['count_sell'] > 0) & (df['count_sell'] < 2) & (
            df['count_buy'] < 1) & df['sell_signal'] & (~df['sell_signal'].shift(1).fillna(False)).astype(int)

        # 根据条件筛选
        current_time = pd.Timestamp.now(tz='Asia/Shanghai')
        condition = (df['buysignal'] | df['sellsignal']) & (
            df.index > current_time - datetime.timedelta(hours=12))

        filtered_data = df[condition].tail(1)

        if len(filtered_data) > 0:
            singal = filtered_data.tail(1).iloc[0]
            signal_type = '买入信号' if singal['buysignal'] else '卖出信号'
            message = f"{symbol}\n"
            message += f"{singal.name.strftime('%Y-%m-%d %H:%M:%S')} {
                signal_type}\n"
            message += f"价格:{singal['close']},位于"
            message += f"ema200上方\n" if singal['up_on_ema200'] else "ema200下方\n"
            macd = '正' if singal['macd_hist'] > 0 else '负'
            message += f"macd值:{macd}"
            send_message(message)
            print(message)


while True:
    job()
    print("任务结束")
    time.sleep(60*60*4)
