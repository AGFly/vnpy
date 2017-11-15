# -*- coding: utf-8 -*-
# author: Andy Guo

'''

author: Andy Guo

将历史tick线数据(目前是RB)形成的分段数据库合并成新的合并数据库，

后续每条新增记录进行添加

'''

# ToDo:  长假前最后一天的交易日 没有15:00这根bar, 会遗漏最后一根bar

import pymongo


class CTAtickData(object):
	def __init__(self):
		self.time = ''
		self.code = ''
		self.lastPrice = 0.0
		self.volume = 0
		self.id = 0


class TickInsert(object):
	def __init__(self):
		client = pymongo.MongoClient()
		self.db = client['DB_Tick_Testing']
		
		collection2013 = 'Collection_Tick_Testing_2013'
		collection2014 = 'Collection_Tick_Testing_2014'
		collection2015 = 'Collection_Tick_Testing_2015'
		collection2016 = 'Collection_Tick_Testing_2016'
		collection2017 = 'Collection_Tick_Testing_2017'
		
		self.collections = [collection2013, collection2014, collection2015, collection2016,
		                    collection2017]
		
		self.targetcollection = self.db['Collection_Tick_Testing_fulldata']
	
		self.db.drop_collection(self.targetcollection)  # todo:  非标项目 第一次使用时保留，以后应该是在原数据库内插入；
	
	def ticks_compile(self):
		
		if not self.targetcollection.find_one():
			id_updating = 1
			for i in self.collections:
				self.ticks_insert(i, id_updating)
				id_updating = self.targetcollection.find().sort('_id', -1)[0]['_id'] + 1
			
			# self.ticks_insert(self.id)
		else:
			result = self.targetcollection.find().sort('_id', -1)
			self.id = result[0]['_id']
			print self.id
	
	def ticks_insert(self, collection, id_updating):
		
		tick = CTAtickData()
		
		workingcollection = self.db[collection]
		records = workingcollection.find()
		
		for record in records:
			tick.volume = record['volUME']
			tick.time = record['time']
			tick.lastPrice = record['lastPrice']
			tick.code = record['code']
			tick.id = id_updating
			flt = {'_id': tick.id}
			self.targetcollection.update(flt, tick.__dict__, upsert=True)
			
			print '已经插入' + str(tick.time) + '的tick数据'
			
			id_updating += 1
	
	def ticks_update(self, collection, id_updating):
		
		
		self.ticks_insert(collection, id_updating)


if __name__ == '__main__':
	
	a = TickInsert()
	# collection_to_update = 'Collection_Tick_Testing_newRecord'
	a.ticks_compile()
	
	# id_to_update = a.targetcollection.find().sort('_id', -1)[0]['_id'] + 1
	
	# a.ticks_update(collection_to_update, id_to_update)
