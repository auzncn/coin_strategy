import ccxt
import pandas as pd
import talib
import os



# Periods = input(title="ATR Period", type=input.integer, defval=10)
# src = input(hl2, title="Source")
# Multiplier = input(title="ATR Multiplier", type=input.float, step=0.1, defval=3.0)
# changeATR= input(title="Change ATR Calculation Method ?", type=input.bool, defval=true)
# showsignals = input(title="Show Buy/Sell Signals ?", type=input.bool, defval=true)
# highlighting = input(title="Highlighter On/Off ?", type=input.bool, defval=true)
# atr2 = sma(tr, Periods)
# atr= changeATR ? atr(Periods) : atr2
# up=src-(Multiplier*atr)
# up1 = nz(up[1],up)
# up := close[1] > up1 ? max(up,up1) : up
# dn=src+(Multiplier*atr)
# dn1 = nz(dn[1], dn)
# dn := close[1] < dn1 ? min(dn, dn1) : dn
# trend = 1
# trend := nz(trend[1], trend)
# trend := trend == -1 and close > dn1 ? 1 : trend == 1 and close < up1 ? -1 : trend
# upPlot = plot(trend == 1 ? up : na, title="Up Trend", style=plot.style_linebr, linewidth=2, color=color.green)
# buySignal = trend == 1 and trend[1] == -1
# plotshape(buySignal ? up : na, title="UpTrend Begins", location=location.absolute, style=shape.circle, size=size.tiny, color=color.green, transp=0)
# plotshape(buySignal and showsignals ? up : na, title="Buy", text="Buy", location=location.absolute, style=shape.labelup, size=size.tiny, color=color.green, textcolor=color.white, transp=0)
# dnPlot = plot(trend == 1 ? na : dn, title="Down Trend", style=plot.style_linebr, linewidth=2, color=color.red)
# sellSignal = trend == -1 and trend[1] == 1

exchange = ccxt.binance({
    'proxies': {
        'http': 'http://localhost:1080',
        'https': 'http://localhost:1080',
    },
})

def get_max_range(row):
    return max(row['true_range1'], row['true_range2'], row['true_range3'])

def get_max(row):
    return max(row['true_range1'], row['true_range2'], row['true_range3'])
def nz_up(row):
    if pd.isna(row['up1']):
        return row['up']
    else:
        return row['up1']
def nz_dn(row):
    if pd.isna(row['dn1']):
        return row['dn']
    else:
        return row['dn1']
def max_up(row):
    if row['close1'] > row['up1']:
        return max(row['up'],row['up1'])
    else:
        return row['up']
def min_dn(row):
    if row['close1'] < row['dn1']:
        return min(row['dn'],row['dn1'])
    else:
        return row['dn']
def buySignal(row):
    # trend == 1 and trend[1] == -1
    return row['trend'] == 1 and row['trend1'] == -1
def sellSignal(row):
    # trend == 1 and trend[1] == -1
    return row['trend'] == -1 and row['trend1'] == 1

def atr(data, window):
    high = data['high']
    low = data['low']
    close = data['close']
    tr1 = abs(high - low)
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1/window, min_periods=window).mean()
    data['atr'] = atr
    return data

def update_trend(df):
    # 初始化trend的第一行值
    df.loc[0,'dn'] = df.loc[0,'hl2']+(3*df.loc[0,'atr'])
    df.loc[0,'dn1'] = df.loc[0,'dn']
    df.loc[0,'dn2'] = df.loc[0,'dn']
    # df.loc[0, "trend"] = 1
    # 遍历df的每一行
    # print(df.loc[0])
    for i in range(1, len(df)):
        dn = df.loc[i,'hl2']+(3*df.loc[i,'atr'])
        if i == 1:
            df.loc[i,'dn'] = dn
            df.loc[i,'dn1'] = df.loc[i-1,'dn'] 
            df.loc[i,'dn2'] = min(df.loc[i,'dn'],df.loc[i,'dn1'])
        else:
            pre_close = df.loc[i-1,'close']
            df.loc[i,'dn1'] = df.loc[i-1,'dn2']
            df.loc[i,'dn'] = dn
            if pre_close < df.loc[i,'dn1']:
                df.loc[i,'dn2'] = df.loc[i,'dn1'] 
            else:
                df.loc[i,'dn2'] = df.loc[i,'dn'] 
        # print(df.loc[i])
        # 获取前一行和当前行的值
        # prev_value = df.loc[i-1, "trend"]
        # prev_up1 = df.loc[i-1, "up1"]
        # prev_dn1 = df.loc[i-1, "dn1"]
        
        # up := close[1] > up1 ? max(up,up1) : up
        # dn := close[1] < dn1 ? min(dn, dn1) : dn



        # if prev_close > df.loc[i,'up1']:
        #     curr_max_up = max(df.loc[i,'up'],df.loc[i,'up1'])
        # else:
        #     curr_max_up = df.loc[i-1,'max_up']
        
        # if prev_close < df.loc[i-1,'dn']:
        #     curr_min_dn = min(df.loc[i,'dn'],df.loc[i-1,'dn'])
        # else:
        #     curr_min_dn = df.loc[i-1,'min_dn']
            
        # # df.loc[i, "max_up"] = curr_max_up
        # df.loc[i, "min_dn"] = curr_min_dn

        # # df.loc[i, "up1"] = curr_max_up
        # df.loc[i, "dn1"] = df.loc[i-1,'min_dn']

        # 根据条件更新当前行的值
        # if df.loc[i, "close"] > df.loc[i, "min_dn"]:
        #     curr_value = 1
        # elif df.loc[i, "close"] < df.loc[i, "max_up"]:
        #     curr_value = -1
        # else:
        #     curr_value = prev_value
        # # 更新当前行的trend值
        # df.loc[i, "trend"] = curr_value
    return df

symbol = 'BTC/USDT'
timeframe = '4h' # 4小时
limit = 3000 # 获取最近的1000个K线数据

klines = exchange.fetch_ohlcv(symbol, timeframe=timeframe)
df = pd.DataFrame(data=klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
# df.set_index('timestamp', inplace=True)
# (最高价 + 最低价)/2
df['hl2'] = (df['high'] +df['low'])/2
df['close1'] =df['close'].shift(1)
#max(high - low, abs(high - close[1]), abs(low - close[1]))
df['true_range1'] = df['high'] -df['low']
df['true_range2'] = abs(df['high'] - df['close1'])
df['true_range3'] = abs(df['low'] - df['close1'])
df['true_range'] = df.apply(get_max_range, axis=1)
# df['atr'] = df['true_range'].rolling(window=10).mean()
df = atr(df,10)

# df['up']=df['hl2']-(3*df['atr'])
# df['up1'] = df['up'].shift(1)
# df['up1'] = df.apply(nz_up, axis=1)
# df['max_up'] = df.apply(max_up, axis=1)

# df['dn']=df['hl2']+(3*df['atr'])
# df['dn1'] = df['dn'].shift(1)
# df['dn1'] = df.apply(nz_dn, axis=1)
# df['min_dn'] = df.apply(min_dn, axis=1)
new_df = df.loc[df['atr'].notnull()]
new_df =new_df.reset_index(drop=True)
new_df = update_trend(new_df)
print(new_df)
# file_path 需要保存的文件路径
# table pandas DataFrame对象
 
new_df.to_excel(r'C:\Users\wz\Desktop\1.xlsx', index=False)
# # buySignal = trend == 1 and trend[1] == -1
# df['trend1'] = df['trend'].shift(1)
# df['buySignal'] =df.apply(buySignal, axis=1)
# df['sellSignal'] =df.apply(sellSignal, axis=1)

# print(df)
# a = df[df['timestamp'] >= '2023-03-10 4:00:00']
# print(df.head(50))
# print(df[df['buySignal']])
# print(df[df['sellSignal']])
# print(df[df['timestamp'] == '2023-03-22 16:00:00'])
# df['trend'] = 1
# df['trend'] = df.apply(trend, axis=1)


# df['trend1'] = df['trend'].shift(1)


# print(df)
# print(trand_1)
# for i in range(len(trand_1)):
#     print(trand_1.iloc[i])
# print(df.columns)
# print(df[df['timestamp'] >= '2023-03-22 16:00:00'])
# print("-----------------------buySignal---------------------------")
# print(df[df['buySignal']])
# print("----------------------sellSignal----------------------------")
# print(df[df['sellSignal']])
