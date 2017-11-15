# -*- coding: utf-8 -*-
# author: Andy Guo

'''
Author:  Andy Guo

这个文件是从tick database 整合成Bar

'''

# todo: check为何会有datetime 为5毫秒的bar线

# todo: 考虑如果tick.time在数据库中不是以升序排列的情况
# todo: 写成class形式
# todo: 整合成30分钟K线

import pymongo
from pprint import pprint
import datetime


class CTABARDATA_AG(object):
	def __init__(self):
		self.symbol = ''
		self.open = 0.0
		self.close = 0.0
		self.high = 0.0
		self.low = 0.0
		self.datetime = ''
		self.volume = 0


class KBar(object):
	def __init__(self):
		
		self.client = pymongo.MongoClient()
		self.db = self.client['DB_Tick_Testing']
		self.collection = self.db['Collection_Tick_Testing_fulldata']
		self.collection_bar = self.db['Collection_Bar_Testing']
		
		self.db.drop_collection(self.collection_bar)  # 第一次需要drop
		
		self.kBar = None
		
		if not self.collection_bar.find_one():
			self.barMinute = None
			self.barHour = None
		else:
			self.barMinute = self.collection_bar.find().sort('time', -1)[0]['time'].minute
			self.barHour = self.collection_bar.find().sort('time', -1)[0]['time'].hour
	
	def bar_generation(self, tick):
		
		tickMin = tick[u'time'].minute
		tickHour = tick[u'time'].hour
		
		special_case = (tickHour == 13 and tickMin == 30 and self.barHour != 13) or (
			tickHour == 21 and tickMin == 00 and self.barHour != 21) or (
			               tickHour == 9 and tickMin == 00 and self.barHour != 9)
		
		if (tickMin != self.barMinute) or special_case:
			
			if self.kBar is None:
				self.kBar = CTABARDATA_AG()
				self.kBar.symbol = tick[u'code']
				self.kBar.open = tick[u'lastPrice']
				self.kBar.close = tick[u'lastPrice']
				self.kBar.high = tick[u'lastPrice']
				self.kBar.low = tick[u'lastPrice']
				self.kBar.datetime = tick[u'time']
				self.kBar.volume = tick[u'volume']
				self.barMinute = tickMin
				self.barHour = tickHour
			else:
				flt = {'_id': tick[u'time']}  # id无所谓
				self.collection_bar.update(flt, self.kBar.__dict__, upsert=True)
				print '生成' + str(self.kBar.datetime) + '的数据'
				self.kBar.symbol = tick[u'code']
				self.kBar.open = tick[u'lastPrice']
				self.kBar.close = tick[u'lastPrice']
				self.kBar.high = tick[u'lastPrice']
				self.kBar.low = tick[u'lastPrice']
				self.kBar.datetime = tick[u'time']
				self.kBar.volume = tick[u'volume']
				self.barMinute = tickMin
				self.barHour = tickHour
		else:
			self.kBar.high = max(self.kBar.high, tick[u'lastPrice'])
			self.kBar.low = min(self.kBar.low, tick[u'lastPrice'])
			self.kBar.close = tick[u'lastPrice']
			self.kBar.volume = self.kBar.volume + tick[u'volume']


class Kbar_TimeScale_Gen(object):
	def __init__(self, db, collection_timescale):
		self.client = pymongo.MongoClient()
		self.db = self.client[db]
		self.collection_bar_timescale = self.db[collection_timescale]
		self.bar_timescale = None
		self.generation_start = False
	
	def clean_collection(self):
		self.db.drop_collection(self.collection_bar_timescale)
	
	def onbar(self, bar):
		
		'''定义所有30Min Bar开始的时间点'''
		
		H09M00 = bar['datetime'].hour == 9 and bar['datetime'].minute == 00
		H09M30 = bar['datetime'].hour == 9 and bar['datetime'].minute == 30
		H10M00 = bar['datetime'].hour == 10 and bar['datetime'].minute == 00
		H10M30 = bar['datetime'].hour == 10 and bar['datetime'].minute == 30
		H11M00 = bar['datetime'].hour == 11 and bar['datetime'].minute == 00
		H13M30 = bar['datetime'].hour == 13 and bar['datetime'].minute == 30
		H14M00 = bar['datetime'].hour == 14 and bar['datetime'].minute == 00
		H14M30 = bar['datetime'].hour == 14 and bar['datetime'].minute == 30
		H21M00 = bar['datetime'].hour == 21 and bar['datetime'].minute == 00
		H21M30 = bar['datetime'].hour == 21 and bar['datetime'].minute == 30
		H22M00 = bar['datetime'].hour == 22 and bar['datetime'].minute == 00
		H22M30 = bar['datetime'].hour == 22 and bar['datetime'].minute == 30
		H23M00 = bar['datetime'].hour == 23 and bar['datetime'].minute == 01  # 只有夜盘有这种情况，因此2300数据归到上一根
		H23M30 = bar['datetime'].hour == 23 and bar['datetime'].minute == 30
		H24M00 = bar['datetime'].hour == 00 and bar['datetime'].minute == 00
		H00M30 = bar['datetime'].hour == 00 and bar['datetime'].minute == 30
		
		Bar_Start = H09M00 or H09M30 or H10M00 or H10M30 or H11M00 or H13M30 or H14M00 or H14M30 \
		            or H21M00 or H21M30 or H22M00 or H22M30 or H23M00 or H23M30 or H24M00 or H00M30
		
		# todo: ending_time要根据进行合称的最后时间进行修改，否则无法合成最后一根K线
		
		ending_time = bar['datetime'].year == 2017 and bar['datetime'].month == 9 \
		              and bar['datetime'].day == 29 and bar['datetime'].hour == 14 \
		              and bar['datetime'].minute == 59
		
		''' 特殊情况做特殊剔除处理'''
		# morning_break = bar['datetime'].minute == 15 and bar['datetime'].hour == 10
		# noon_break = bar['datetime'].minute == 30 and bar['datetime'].hour == 11
		# day_break = bar['datetime'].minute == 00 and bar['datetime'].hour == 15
		# night_break = bar['datetime'].minute == 00 and bar['datetime'].hour == 23
		# special_nightbreak = bar['datetime'].minute == 00 and bar['datetime'].hour == 01
		#
		# morning_break_pre = bar['datetime'].minute == 14 and bar['datetime'].hour == 10
		# noon_break_pre = bar['datetime'].minute == 29 and bar['datetime'].hour == 11
		# day_break_pre = bar['datetime'].minute == 59 and bar['datetime'].hour == 14
		# night_break_pre = bar['datetime'].minute == 59 and bar['datetime'].hour == 22
		# special_nightbreak_pre = bar['datetime'].minute == 59 and bar['datetime'].hour == 00
		#
		# scale_match = (bar['datetime'].minute + 1) % self.time_scale == 0
		# special_match = morning_break or day_break or night_break or noon_break or special_nightbreak
		# special_match_pre = morning_break_pre or day_break_pre or night_break_pre or noon_break_pre or special_nightbreak_pre
		
		if not ending_time:
			if not self.generation_start:
				
				self.bar_timescale = CTABARDATA_AG()
				self.bar_timescale.symbol = bar['symbol']
				self.bar_timescale.open = bar['open']
				self.bar_timescale.high = bar['high']
				self.bar_timescale.low = bar['low']
				self.bar_timescale.close = bar['close']
				self.bar_timescale.datetime = bar['datetime']
				
				self.generation_start = True
			
			else:
				if Bar_Start and self.bar_timescale:
					
					flt = {'_id': self.bar_timescale.datetime}
					self.collection_bar_timescale.update(flt, self.bar_timescale.__dict__, upsert=True)
					print '生成' + str(self.bar_timescale.datetime) + '的数据'
					self.bar_timescale = None
					
					self.bar_timescale = CTABARDATA_AG()
					self.bar_timescale.symbol = bar['symbol']
					self.bar_timescale.open = bar['open']
					self.bar_timescale.high = bar['high']
					self.bar_timescale.low = bar['low']
					self.bar_timescale.close = bar['close']
					self.bar_timescale.datetime = bar['datetime']
				
				else:
				
					self.bar_timescale.high = max(self.bar_timescale.high, bar['high'])
					self.bar_timescale.low = min(self.bar_timescale.low, bar['low'])
					self.bar_timescale.close = bar['close']
		else:
			
			self.bar_timescale.high = max(self.bar_timescale.high, bar['high'])
			self.bar_timescale.low = min(self.bar_timescale.low, bar['low'])
			self.bar_timescale.close = bar['close']
			
			flt = {'_id': self.bar_timescale.datetime}
			self.collection_bar_timescale.update(flt, self.bar_timescale.__dict__, upsert=True)
			print '生成' + str(self.bar_timescale.datetime) + '的数据'


class refer(object):
	def __init__(self):
		print 'successful'


if __name__ == '__main__':
	''' 按照需求进行Kbar的生产 或者  其他分钟线的合成'''
	''' 这部分第一次使用时保留，后续不需要initiate'''
	# bar_initial = KBar()
	#
	# for i in bar_initial.collection.find():
	# 	bar_initial.bar_generation(i)
	
	''' 生成30分钟K线  '''
	collection_base = pymongo.MongoClient()['DB_Tick_Testing']['Collection_Bar_Testing']
	
	bar_combined = Kbar_TimeScale_Gen('DB_Tick_Testing', 'Collection_30Min')
	bar_combined.clean_collection()
	
	'''这个版本针对30Min K线合称， 需要其他时间框架 需要更新代码中的Bar_Start计算'''
	for i in collection_base.find():
		bar_combined.onbar(i)
