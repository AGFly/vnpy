# -*- coding: utf-8 -*-
# author: Andy Guo

# TOdo:  1.需要比较从缓存列表中读取的熟读和从数据库中读取的速度
# Todo:  2.设计指针的悬浮窗口
# todo:  3.有新的tick进来触发的处理
# todo:  5.安排倒退的数据读取法
# todo:  6.plotText
# todo:  7.从cta服务器上接受数据
# todo:  8.横坐标变成时


import pymongo
import pyqtgraph as pg
import sys
from PyQt4.QtGui import *
from PyQt4.QtCore import *
from dateutil.parser import parse


class PriceW(QWidget):
	#	signal = pyqtSignal(type(Event()))
	
	date_start = parse('20170101 09:00:00')
	
	# K线图参数设置
	EMAfastAlpha = 0.0167
	EMAslowAlpha = 0.0083
	fastEMA = 0
	slowEMA = 0
	listfastEMA = []
	listslowEMA = []
	
	# 缓存K线数据对象列表
	
	listBar = []
	listOpen = []
	listClose = []
	listHigh = []
	listLow = []
	listVolume = []
	
	# 是否完成历史数据读取
	# initCompleted = False
	
	class CandlestickItem(pg.GraphicsObject):
		def __init__(self, data):
			pg.GraphicsObject.__init__(self)
			# super(CandlestickItem, self).__init__()
			# self.data = (1, 3000, 3050, 2990, 3100)
			self.data = data
			self.generatePicture()
		
		def generatePicture(self):
			self.picture = QPicture()
			p = QPainter(self.picture)
			p.setPen(pg.mkPen(color='w', width=0.4))
			w = 0.2
			for (t, open, close, min, max, vol) in self.data:
				
				p.drawLine(QPoint(t, min), QPoint(t, max))
				if open > close:
					p.setBrush(pg.mkBrush('g'))  # Green
				else:
					p.setBrush(pg.mkBrush('r'))  # Red
				p.drawRect(QRectF(t - w, open, w * 2, close - open))
			p.end()
		
		def paint(self, p, *args):
			p.drawPicture(0, 0, self.picture)
		
		def boundingRect(self):
			return QRectF(self.picture.boundingRect())
	
	def __init__(self):
		super(PriceW, self).__init__()
		# QWidget.__init__(self)   也可以
		self.initUI()
		self.data_get()
	
	def initUI(self):
		self.setWindowTitle(u'Price')
		
		# K线初始化
		self.vb = QVBoxLayout()
		self.initplotK()  # K线初始化
		self.initplotTendency()  # 副图初始化
		
		# 整体布局
		self.hb = QHBoxLayout()
		self.hb.addLayout(self.vb)
		self.setLayout(self.hb)
	
	def data_get(self):  # 直接在这里完成历史数据的插入
		client = pymongo.MongoClient()
		db = client['DB_Tick_Testing']
		collection = db['Collection_30Min']
		
		count = 1
		for i in collection.find({'datetime': {'$gt': self.date_start}}):
			if i[u'open'] > 0:
				# print i
				self.onbar(count, i[u'open'], i[u'close'], i[u'low'], i[u'high'], i[u'volume'])
				count += 1
	
	def initplotK(self):
		self.pw = pg.PlotWidget(name='Plot')  # K线图
		self.vb.addWidget(self.pw)
		self.pw.setDownsampling(mode='peak')
		self.pw.setClipToView(True)
		
		self.curve1 = self.pw.plot()  # fastEMA 对象
		self.curve2 = self.pw.plot()  # slowEMA 对象
		
		# self.curve1 = self.pw.plot()
		# self.curve2 = self.pw.plot()
		
		self.candle = self.CandlestickItem(self.listBar)  # print 1 次
		self.pw.addItem(self.candle)
	
	# self.arrow = pg.ArrowItem()
	# self.pw.addItem(self.arrow)
	
	def initplotTendency(self):
		self.pw2 = pg.PlotWidget(name='Plot2')
		self.vb.addWidget(self.pw2)
		self.pw2.setDownsampling(mode='peak')
		self.pw2.setClipToView(True)
		self.pw2.setMaximumHeight(200)
		self.pw2.setXLink('Plot')
		self.curve3 = self.pw2.plot()
	
	def plotK(self):
		
		self.curve1.setData(self.listfastEMA, pen=(255, 0, 0), name='Red Curve')
		self.curve2.setData(self.listslowEMA, pen=(0, 255, 0), name='Green Curve')
		self.pw.removeItem(self.candle)
		self.candle = self.CandlestickItem(self.listBar)
		self.pw.addItem(self.candle)
	
	def plotTendency(self):
		self.curve3.setData(self.listVolume, pen=(255, 255, 255), name='White curve')
	
	def onbar(self, n, o, c, l, h, v):
		
		self.listBar.append((n, o, c, l, h, v))
		self.listOpen.append(o)
		self.listClose.append(c)
		self.listHigh.append(h)
		self.listLow.append(l)
		self.listVolume.append(v)
		
		# 计算K线EMA均线
		if self.fastEMA:
			self.fastEMA = c * self.EMAfastAlpha + self.fastEMA * (1 - self.EMAfastAlpha)
			self.slowEMA = c * self.EMAslowAlpha + self.slowEMA * (1 - self.EMAslowAlpha)
		
		else:
			self.fastEMA = c
			self.slowEMA = c
		self.listfastEMA.append(self.fastEMA)
		self.listslowEMA.append(self.slowEMA)
		
		# self.listBar.append((n, o, c, l, h))
		# print self.listBar
		self.plotK()  # 画K线图
		self.plotTendency()  # 画成交量副图


class mainwindow(QMainWindow):
	def __init__(self):
		super(mainwindow, self).__init__()
		self.initUI()
	
	def initUI(self):
		self.setWindowTitle('Main Window')
		self.price = PriceW()
		# self.price.onbar()
		
		mkttb = QTabWidget()
		mkttb.addTab(self.price, u'行情')
		
		upBox = QHBoxLayout()
		upBox.addWidget(mkttb)
		
		vBox = QVBoxLayout()
		vBox.addLayout(upBox)
		
		centralwidget = QWidget()
		centralwidget.setLayout(vBox)
		self.setCentralWidget(centralwidget)
		self.show()


def main():
	app = QApplication(sys.argv)
	
	PriceTesting = mainwindow()
	
	sys.exit(app.exec_())


if __name__ == '__main__':
	main()
