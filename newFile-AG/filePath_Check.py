# -*- coding: utf-8 -*-
# author: Andy Guo

import pymongo
import os
import re

'''
class tools_AG(object):
	def __init__(self):
		self.client = pymongo.MongoClient()
		self.db = self.client['DB_Tick_Testing']
	
	def db_recording_check(self):
		collection = self.db['Collection_Tick_Testing_fulldata']
		findings = collection.find().sort('_id', -1)
		print findings[0]


if __name__ == '__main__':
	tool = tools_AG()
	tool.db_recording_check()

'''

list_file =[]

contract = u'rb主力连续'

regex = re.compile(r'%s.*' % contract)

root_path = 'D:\Future Data\Monthly Data 2017'

for parent_name, dir_name, file_name in os.walk(root_path):
	
	for i in range(len(file_name)):
	
		file_name[i]=file_name[i].decode('GB2312')
		# print file_name[i]
	
	for filename in file_name:
		filepath = os.path.join(parent_name, filename)
		# print filepath
		if regex.search(filepath):
			list_file.append(filepath)

for i in range(len(list_file)):
	print list_file[i]
	