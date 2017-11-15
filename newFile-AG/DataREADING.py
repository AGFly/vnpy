# -*- coding: utf-8 -*-
# author: Andy Guo
import csv
from datetime import datetime

with open('rb1801.csv', 'r') as f:
	reader = csv.DictReader(f)
	
# 需要查重复
	
	itemDict = {}
	timeList =[]
	for row in reader:
		itemDict.setdefault(row['datetime'], []).append(row['lastPrice'])
		
	for k, v in itemDict.items():
		if len(v) >1 and k >= '2017-10-10T22:34:14.000Z':
			timeList.append(k)
		
	for time in timeList:
		print time + '  ' + itemDict[time]


'''
	i = 0
	for row in reader:
		if i <= 1:
			print datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
			i += 1
		else:
			break

d1 = {}
key =1
value1 =2
d1.setdefault(key, []).append(value1)
print d1

value2 = 3
d1.setdefault(key, []).append(value2)
print d1

'''