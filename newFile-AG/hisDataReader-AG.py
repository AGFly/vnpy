# -*- coding: utf-8 -*-
# author: Andy Guo

'''
Author: Andy Guo

从已经下载的历史CSV数据切片，清洗后

存入分段数据库，以便"TickInsert-AG.py"进行合并

'''

import pymongo
import csv
from dateutil.parser import parse
import os
import re


class CTAtickData(object):
	def __init__(self):
		self.time = ''
		self.code = ''
		self.lastPrice = 0.0
		self.volUME = 0


class FileGen(object):
	# def file_search_origin(self, file_path=None):
	#
	# 	list_file = []
	#
	# 	for i in os.listdir(file_path):
	# 		list_file.append(os.path.join(file_path, i.decode('GB2312')))
	# 	return list_file
	
	'''用于每日行情数据的更新，注意contract要手动选择主连'''
	'''以及用于查找带中文字体的主力合约'''
	
	def file_search(self, contract, root_path=None):
		
		list_file = []
		
		regex = re.compile(r'%s.*' % contract)
		
		for parent, dirnames, filenames in os.walk(root_path):
			
			for i in range(len(filenames)): filenames[i] = filenames[i].decode('GB2312')
			
			for filename in filenames:
				filepath = os.path.join(parent, filename)
				if regex.search(filepath):
					list_file.append(filepath)
		
		return list_file


class HisDataGen(object):
	def __init__(self, db):
		
		self.db = db
		
		'''设置Tick字段'''
	
	def new_collection(self, list_file, collection, id_record):
		
		self.db.drop_collection(collection)
		
		for filename in list_file:
			
			reader = csv.DictReader(file(filename, 'r'))
			
			reader.fieldnames[1] = 'Code'
			reader.fieldnames[2] = 'Time'
			reader.fieldnames[3] = 'Last'
			reader.fieldnames[7] = 'Volume'
			
			tick = CTAtickData()
			
			for row in reader:
				tick.code = str(row['Code'])
				tick.time = parse(row['Time'])
				tick.lastPrice = float(row['Last'])
				tick.volUME = float(row['Volume'])
				tick._id = id_record
				
				flt = {'_id': tick._id}
				collection.update(flt, tick.__dict__, upsert=True)
				print '正在存入第' + str(id_record) + '条记录' + str(tick.time)
				id_record += 1
	
	'''生成一个toupdate的collection 存入csv文件，再存每日定期入大库中'''
	
	def new_record(self, file_list, collection, id_record):
		self.new_collection(file_list, collection, id_record)


if __name__ == '__main__':
	''' initiate 相关数据参数'''
	client = pymongo.MongoClient()
	target_db = client['DB_Tick_Testing']
	root_path = 'D:\Future Data\Monthly Data 2017'
	contract_toSearch = u'rb主力'    '''中文描述的文件名'''
	
	file_list = FileGen().file_search(contract_toSearch, root_path)
	
	collection_toHandle = target_db['Collection_Tick_Testing_2017']
	
	start_id = 1
	''' initiate 相关数据参数完成'''
	
	hisData = HisDataGen(target_db)  # 生成实例
	hisData.new_collection(file_list, collection_toHandle, start_id)  # 生成分段数据库


# # collection_NewCollection = target_db['Collection_Tick_Testing_2016']
# collection_NewRecord = target_db['Collection_Tick_Testing_newRecord']
#
# # path_NewCollection = 'F:\\Future Data\\For Testing\\2016'
# # path_NewRecord = 'F:\\Future Data\\For Testing\\toupdate'
#
# start_id = 1
#
# hisData = HisDataGen(target_db)
#
# hisData.new_record(file_list, collection_NewRecord, start_id)

# hisData.new_collection(path_NewCollection, collection_NewCollection, start_id)   #择需使用
