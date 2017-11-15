# -*- coding: utf-8 -*-
# author: Andy Guo
import pymongo
from pandas import DataFrame, Series
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt

''' 定义基础设施'''
''' 本地DB操作'''
client = pymongo.MongoClient()
db = client['DB_Tick_Testing']
collection = db['Collection_30Min']

ticks = collection.find().sort('_id', -1)

for i in range(2):
	
	print tick
	# year = ticks[i]['datetime'].year
	# month = ticks[i]['datetime'].month
	# day = ticks[i]['datetime'].day
	#
	# # print year, month, day
	#
	# date_dt = dt.date(year, month, day)
	#
	# print date_dt

'''阿里云DB操作'''

# client = pymongo.MongoClient('mongodb://101.132.35.222:27017/')
# db = client['VnTrader_Tick_Db']
#
# print db.collection_names()

# db['rb1801'].drop()
