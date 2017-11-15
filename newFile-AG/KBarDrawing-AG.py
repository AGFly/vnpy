# -*- coding: utf-8 -*-
# author: Andy Guo
''' 尝试用pyecharts画图， 需要复制代码到jupyter notebook中使用'''
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from matplotlib.finance import candlestick_ohlc, candlestick2_ohlc
from matplotlib.dates import DateFormatter, WeekdayLocator, DayLocator, MONDAY, YEARLY
from matplotlib.dates import MonthLocator, MONTHLY
import datetime as dt
import pylab
import pymongo
import matplotlib.ticker as ticker
from pyecharts import Kline, Line, Overlap
import talib as ta

client = pymongo.MongoClient()
db = client['DB_Tick_Testing']
collection = db['Collection_30Min']

ticks = collection.find()

''' Step 1 先整理出 pyecharts 需要的x轴- timedata 和 y轴 OCLH 列表'''
data_OCLH = []
close_data = []
time_data = []

signal = []  # 设置信号记录点

for i in range(0, 5000):  # todo:  size可变， ex: collection.count()
	tick = ticks[i]
	data_OCLH.append([tick['open'], tick['close'], tick['low'], tick['high']])
	time_data.append(tick['datetime'])
	close_data.append(tick['close'])

signal1 = {'coord': [time_data[3698], close_data[3698]+50], 'name': 'Open'}    # todo: 信号记录的格式，可以后续继续美工

signal.append(signal1)

''' 做K线图 '''

kLine = Kline('螺纹钢K线图')
kLine.add('30分钟图', time_data, data_OCLH, is_datazoom_show=True, datazoom_type='both', mark_point=signal,
          mark_point_symbol='triangle', mark_point_symbolsize= 15)

kLine

''' 做均线图 '''
real_data = np.array(close_data)

MA10 = ta.MA(real_data, timeperiod=10).tolist()

line = Line('螺纹钢均线图')
line.add('MA10', time_data, MA10, is_datazoom_show=True, datazoom_type='both', line_curve=1, tooltip_tragger='axis',
         tooltip_tragger_on='mousemove|click')

line

''' 合称在一张图中  '''
overLap = Overlap()
overLap.add(kLine)
overLap.add(line)
overLap




# fig, ax = plt.subplots()
#
# candlestick2_ohlc(ax, open_data, high_data, low_data, close_data, width= 0.3)
#
# ax.xaxis.set_major_locator(ticker.MaxNLocator(2))
#
# def mydate(x,pos):  # todo 研究这段代码的意思
#     try:
#         return time_data[int(x)]
#     except IndexError:
#         return ''
# ax.xaxis.set_major_formatter(ticker.FuncFormatter(mydate))
#
# fig.autofmt_xdate()
# fig.tight_layout()
#
# plt.show()
#
#
# # ''' Step 2 再整理出list 格式的 Dates'''
# #
# # time_K = []
# # for i in range(0, collection.count()):
# # 	time_K.append(ticks[i]['datetime'])
# #
# # ''' 结合data, index, columns 生成DataFrame '''
# #
# # returndata = pd.DataFrame(data, index=time_K, columns=['OPEN', 'HIGH', 'LOW', 'CLOSE'])
# #
# # returndata.index.name = 'DateTime'
# #
# # datareshape = returndata.reset_index()
# # datareshape['DateTime'] = mdates.date2num(datareshape['DateTime'].astype(dt.date))  # todo
# #
# # print datareshape
#
# ''' 开始准备绘图'''
