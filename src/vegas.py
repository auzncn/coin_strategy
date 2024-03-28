import ccxt
import pandas as pd
import time
import requests
import json
from retry import retry
import talib
from datetime import datetime

class Market:
    def __init__(self, name, volume):
        self.name = name
        self.volume = volume
    def __str__(self):
        return ("name:%s\tvolume:%d" % (self.name, self.volume))

# 初始化币安交易所
exchange = ccxt.binance({
    'proxies': {
        'http': 'http://localhost:1080',
        'https': 'http://localhost:1080',
    },
})

# def retry(func):
#     def wrapper(*args, **kwargs):
#         count = 0
#         while True:
#             try:
#                 return func(*args, **kwargs)
#             except Exception as e:
#                 count += 1
#                 if count == 5:
#                     raise e
#                 print(f"Error Occured, retrying...Attempts: {count}")
#                 time.sleep(5)
#     return wrapper


def send_message(msg):
   
    headers = {'content-type': 'application/json'}
    data = {'msgtype': 'text', 'text': {'content': msg}}
    requests.post(url=webhook, headers=headers, data=json.dumps(data))

@retry(tries=100, delay=5)
def action():
    print("开始")
    new_break = []
    # on_trend = []
    # watch_out = []
    out_of_trend = []
    very_strong = []
    very_weak = []
    maybe_in = []
    high_volume = []
    divergences = []
    attracts = []
    # 获取所有usdt交易对
    markets = exchange.load_markets()
    # 筛选ema12由底向上穿过ema144和ema169的币种
    for market in markets:
        if "/USDT" in market and markets.get(market)["spot"] and markets.get(market)["active"] and "UP/USDT" not in market and "DOWN/USDT" not in market:
            print(market)
            # 获取4小时K线数据
            time.sleep (exchange.rateLimit / 1000)
            data = exchange.fetch_ohlcv(market, '4h',limit=1000)
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)

            # 计算ema12, ema144, ema169
            df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
            df['ema144'] = df['close'].ewm(span=144, adjust=False).mean()
            df['ema169'] = df['close'].ewm(span=169, adjust=False).mean()
            df['ema576'] = df['close'].ewm(span=576, adjust=False).mean()
            df['ema676'] = df['close'].ewm(span=676, adjust=False).mean()
            # 筛选条件
            # if df['ema12'][-1] > df['ema144'][-1] and df['ema12'][-1] > df['ema169'][-1] :
            #     on_trend.append(market)

            #     # if df['ema12'][-1] > df['ema576'][-1] and df['ema12'][-1] > df['ema676'][-1]:
            #     #     very_strong.append(market)

            #     if df['ema12'][-2] < df['ema144'][-2] and df['ema12'][-2] < df['ema169'][-2]:
            #         new_break.append(market)
            
            # if df['ema12'][-1] > df['ema576'][-1] and df['ema12'][-1] > df['ema676'][-1] :
            
            #     if df['ema12'][-2] < df['ema576'][-2] and df['ema12'][-2] < df['ema676'][-2]:
            #         new_break.append(market)
            
            # if df['close'][-1] < df['ema144'][-1] and df['close'][-1] < df['ema169'][-1]:
            #     watch_out.append(market)

            #     if df['ema12'][-1] < df['ema144'][-1] and df['ema12'][-1] < df['ema169'][-1] and df['ema12'][-2] > df['ema144'][-2] and df['ema12'][-2] > df['ema169'][-2]:
            #         out_of_trend.append(market)

            #     if df['ema12'][-1] < df['ema576'][-1] and df['ema12'][-1] < df['ema676'][-1]:
            #         very_weak.append(market)
            volume = df['volume'][-1]*df['close'][-1]
            df['volume_ma10']  = df['volume'].rolling(window=10).mean()
            if (volume / (df['volume'][-2]* df['close'][-2]) > 8) and df['close'][-2] < df['close'][-1]:
                #突然放大成交量
                high_volume.append(Market(market,volume))

            # df['cma6'] = df['close'].rolling(window=6).mean()
            # df['vma12'] = df['volume'].rolling(window=12).mean()

            # # print(df)

            # # 量价是否出现背离
            # divergence = (df['close'][-2] > df['cma6'][-7]) and (df['volume'][-2] < df['vma12'][-7])
            # if divergence:
            #     divergences.append(market)

            # # 可能吸筹
            # attract = ((abs(df['cma6'][-2] - df['cma6'][-7]) / df['cma6'][-2] < 0.05) and (df['volume'][-2] > df['vma12'][-7]))
            # if attract:
            #     attracts.append(market)

            if df['volume_ma10'][-1] * df['close'][-1] >= 50000:
                if df['ema12'][-1] > df['ema144'][-1] > df['ema169'][-1] > df['ema576'][-1] > df['ema676'][-1]:
                    #多头排列
                    very_strong.append(Market(market,volume))
                    if df['close'][-1] < df['ema144'][-1] and df['close'][-1] < df['ema169'][-1]:
                        #可能进场机会
                        maybe_in.append(Market(market,volume))

                if df['ema12'][-1] < df['ema144'][-1] < df['ema169'][-1] < df['ema576'][-1] < df['ema676'][-1]:
                    #空头排列
                    very_weak.append(Market(market,volume))

                if df['ema12'][-1] < df['ema144'][-1] or df['ema12'][-1] < df['ema169'][-1]:
                    if df['ema12'][-2] > df['ema144'][-2] or df['ema12'][-2] > df['ema169'][-2]:
                        #可能脱离趋势
                        out_of_trend.append(Market(market,volume))

                if df['ema12'][-1] < df['ema576'][-1] or df['ema12'][-1] < df['ema676'][-1]:
                    if df['ema12'][-2] > df['ema576'][-2] or df['ema12'][-2] > df['ema676'][-2]:
                        #可能脱离趋势
                        out_of_trend.append(Market(market,volume))
                
                if df['ema12'][-1] > max(df['ema144'][-1], df['ema169'][-1]):
                    if df['ema12'][-2] < max(df['ema144'][-2], df['ema169'][-2]):
                        #可能突破
                        new_break.append(Market(market,volume))

                if df['ema12'][-1] > max(df['ema576'][-1],df['ema676'][-1]):
                    if df['ema12'][-2] < max(df['ema576'][-2],df['ema676'][-2]):
                        #可能突破
                        new_break.append(Market(market,volume))


    sorted_new_break = sorted(new_break, key=lambda m: m.volume, reverse=True)
    sorted_out_of_trend = sorted(out_of_trend, key=lambda m: m.volume, reverse=True)
    sorted_very_strong = sorted(very_strong, key=lambda m: m.volume, reverse=True)
    sorted_maybe_in = sorted(maybe_in, key=lambda m: m.volume, reverse=True)
    sorted_very_weak = sorted(very_weak, key=lambda m: m.volume, reverse=True)

    if len(sorted_new_break) > 0:
        print("-----------------------新突破------------------------")
        break_msg=','.join([o.name for o in sorted_new_break]).replace("/USDT","")
        print(break_msg)
        send_message("新突破:\n"+break_msg)
        
    if len(sorted_out_of_trend) > 0:
        print("-----------------------脱离趋势------------------------")
        out_trend_msg = ','.join([o.name for o in sorted_out_of_trend]).replace("/USDT","")
        print(out_trend_msg)
        send_message("脱离趋势:\n"+out_trend_msg)

    if len(sorted_very_strong) > 0:
        print("-----------------------多头排列------------------------")
        strong_msg = ','.join([o.name for o in sorted_very_strong]).replace("/USDT","")
        print(strong_msg)
        # send_message(strong_msg)

    if len(sorted_maybe_in) > 0:
        print("-----------------------可能进场机会------------------------")
        maybe_in_msg = ','.join([o.name for o in sorted_maybe_in]).replace("/USDT","")
        print(maybe_in_msg)
        send_message("可能进场:\n"+maybe_in_msg)

    if len(sorted_very_weak) > 0:
        print("-----------------------空头排列------------------------")
        very_weak_msg = ','.join([o.name for o in sorted_very_weak]).replace("/USDT","")
        print(very_weak_msg)
        # send_message(very_weak_msg)

    if len(high_volume) > 0:
        print("-----------------------放大成交量------------------------")
        msg = ','.join([o.name for o in high_volume]).replace("/USDT","")
        print(msg)
        send_message("放量上涨:\n"+msg)

    # if len(divergences) > 0:
    #     print("-----------------------背离------------------------")
    #     msg = ",".join(divergences).replace("/USDT","")
    #     print(msg)

    # if len(attracts) > 0:
    #     print("-----------------------吸筹------------------------")
    #     msg = ",".join(attracts).replace("/USDT","")
    #     print(msg)
    # print("在趋势")
    # print(on_trend)
    # print("-----------------------------------------------")

    # print("多头排列")
    # print(very_strong)
    # print("-----------------------------------------------")

    # print("新突破")
    # print(new_break)
    # print("-----------------------------------------------")

    # print("注意")
    # print(watch_out)
    # print("-----------------------------------------------")

    # print("可能脱离趋势")
    # print(out_of_trend)
    # print("-----------------------------------------------")
    

    # print("空头排列")
    # print(very_weak)
    # print("-----------------------------------------------")
    # # if len(new_break) > 0 :
    #     msg = ",".join(new_break).replace("/USDT","")
    #     send_message("新突破:"+ msg) 
    # if len(very_weak) > 0 :
    #     msg = ",".join(very_weak).replace("/USDT","")
    #     send_message("空头排列:"+ msg) 
    # if len(out_of_trend) > 0 :
    #     msg = ",".join(out_of_trend).replace("/USDT","")
    #     send_message("可能脱离趋势:"+ msg) 
    # if len(very_strong) > 0 :
    #     msg = ",".join(very_strong).replace("/USDT","")
    #     send_message("多头排列:"+ msg)    
while True:
    action()
    time.sleep(3600)
