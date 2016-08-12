#!/usr/bin/env
# -*- coding: utf-8 -*-
# __author__ = 'QY-Y'

import os
import re
import sqlite3
import time
import sys
import Queue
import getopt
import logging
import threading
import signal
import urllib
import urllib2
import traceback
import curses
import lxml.html

class NoneTypeError(Exception): pass

IS_EXIT = False
def handler(signum, frame):
	global IS_EXIT
	IS_EXIT = True
	print "exiting..."
	sys.exit()

def _requestData(url):
	req = urllib2.Request(url)
	req.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/48.0')
	try:
		res = urllib.urlopen(url).read()
	except Exception,e: 
		logging.error('open['+url+']failed '+str(e))
		return req.type, req.host, None
	return req.get_type(), req.get_host(), res

def _initDB(dbFile):
	'''init database file create table(if necessary)
	Args:
		dbFile: Name of the Database file  
	Returns: 
		db: A sqlite3 connection obejct
		c: A cursor from db'''
	exist = False
	ls = os.listdir('.')
	if dbFile in ls:
		exist = True
	db = sqlite3.connect(dbFile, check_same_thread=False)
	c = db.cursor()
	if not exist:
		try:
			c.execute('create table spider(id integer primary key,url text,key text,content text)')
			db.commit()
		except sqlite3.OperationalError:
			logging.cratical(dbFile+'Error in creating table')
	return db, c

def _insert(url, key, content):
	'''insert info to defualt database
	Args:
		url: 
		key: 
		content: 
	'''
	# try:
	# 	content = content.decode('gbk')
	# except UnicodeDecodeError:
	# 	content = content.decode('utf8', 'ignore')
	# 	logging.debug("["+url+'] unicode decode error,using utf-8')
	# content = content.decode('ascii')
	content = urllib.quote(str(content))
	try:
		c.execute('insert into spider(url,key,content) values("'+url+'","'+key+'","'+content+'")')
		db.commit()
	except sqlite3.OperationalError:
		logging.critical('insert ['+url+'] error')


class worker(threading.Thread):
	def __init__(self, links, key, rlock):
		threading.Thread.__init__(self)
		self.queue = links
		self.keyList = key
		self.rlock = rlock
		self.link = None
		self.depth = None
		self.key = None
		self.setDaemon(True)  
		self.start()
		# self.join()
		self.exc_traceback = ''


	def run(self):
		global URLS
		while True:
			try:
				self.link, self.depth = self.queue.get(timeout=2)
				self.key = self.keyList[0]
			except Exception,e:
				self.exc_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
				sys.exit()
				break
			if self.depth > 0:
				self.depth -= 1
				links = self.getLinks()
				if links:
					for i in links:
						if i not in URLS:
							URLS.add(i)
							self.queue.put((i, self.depth))
				self.queue.put((self.link, 0))
			else:
				self.download2DB()
				logging.info(self.link+'  ['+str(self.depth)+']')
			self.queue.task_done()


	def download2DB(self):
		data = _requestData(self.link)[2]
		if not data:
			return
		try:
			html = data.decode('gbk')
		except UnicodeDecodeError:
			html = data.decode('utf8', 'ignore')
		if key in html: 
			self.rlock.acquire()
			_insert(self.link, self.key, data)
			self.rlock.release()

	def getLinks(self):
		resType, resHost, resData = _requestData(self.link)
		if not resData:
			raise NoneTypeError
		try:
			data = resData.decode('gbk')
		except UnicodeDecodeError:
			data = resData.decode('utf8', 'ignore')
		host = resType+'://'+resHost
		doc = lxml.html.document_fromstring(data)
		tags = ['a', 'iframe', 'frame']
		doc.make_links_absolute(host)
		links = doc.iterlinks()
		trueLinks = []
		for l in links:
			if l[0].tag in tags:
				trueLinks.append(l[2])
		return trueLinks
class showProgress(threading.Thread):
	def __init__(self, QLinks, depth, event):
		threading.Thread.__init__(self)
		self.QLinks = QLinks
		self.depth = depth
		self.event = event
		self.setDaemon(True)  
		self.start()
		self.join()

	def run(self):
		# if self.depth == 0:
		# 	print 'level 1 :', 1, '/', 1
		# 	return
		# screen = curses.initscr()  
		# maxFile = [0] * (self.depth+1)
		while True:
			print 'show code'
			time.sleep(10)
		# 	links = list(self.QLinks.__dict__['queue'])
			
		# 	depth = [x[1] for x in links]
		# 	'''keys中元素是[deep值,次数]，
		# 	deep=0为最里子层，deep=n-1为父层'''
		# 	keys = [[x, 0] for x in range(self.deep+1)]
		# 	n = len(keys)
		# 	for d in depth:
		# 		keys[d][1] += 1
		# 	screen.clear()  
		# 	count = 0
		# 	for d in range(1, n+1):
		# 		count += 1
		# 		if keys[n-d][1] > maxFile[d-1]:
		# 			maxFile[d-1] = keys[n-d][1]
		# 		screen.addstr(count, 0, 'level ' + str(d) + ' : ' + 
		# 			str(keys[n-d][1])+' / '+str(maxFile[d-1]))
		# 	screen.refresh()  
		# 	time.sleep(0.2)
		# 	total = functools.reduce(lambda x, y: x + y,
		# 							 [i[1] for i in keys])
		# 	totalMax = functools.reduce(lambda x, y: x + y, maxFile)
			if self.event.is_set():
				curses.endwin()
				line = 'Done at '+time.ctime()
				print line
				logging.info(line)
				break
class threadPool:
	def __init__(self, num, event):
		self.num = num
		self.event = event
		self.threads = []
		self.queue = Queue.Queue()
		self.key = [None]
		self.createThread()

	def createThread(self):
		for i in range(self.num):
			newThread = worker(self.queue, self.key, rlock)
			newThread.setDaemon = True
			self.threads.append(newThread)

		logging.info(str(self.num) + " woreker created")
	def putJob(self, job, key=None):
		self.queue.put(job)

		logging.info(str(job) + " job putted")
		self.key[0] = key

	def getQueue(self):
		return self.queue

	def wait(self):
		self.queue.join()
		self.event.set()
		return
	def showThreads(self):
		return self.threads

def _dealSameFileName(name):
	try:
		files=os.listdir('.')
	except:
		logging.error('无法读取本目录下的文件')
		exit()
	count=1
	while True:
		if name in files:
			name='.'.join([name,str(count)])
			count+=1
		else:
			return name

def _usage():
	'''print usage'''
	
	msg = '''usage:
python spider.py -u [URL] -d [Depth] -f [Log File] -l [Level] --thread [Thread Number] \
--dbfile [Database File Name] --key [Key Word]
-u  Begining URL ,must start with http or https
-d  The depth of the spider ,defalut: [1]
-f  Name(path) of the log file ,default: [spider.log]
-l  How detailed the logging should be(1-5), default: [1]
--thread  The capability of the threading pool, default: [10]
--dbfile  Name of the database file, default: [spider.db]
--key (Optional) Key word
--testself (Optional) Test module
-h  Print this page'''
	print msg

def mainHandler(threadNum, url, depth, keyWord, test):
	event = threading.Event()
	event.clear()
	pool = threadPool(threadNum, event)
	# showProgress(pool.getQueue(), depth, event)
	pool.putJob((url, depth), keyWord)
	pool.wait()
	
	# if test:  
	# 	import test
	# 	test.test(key, dbFile)


if __name__ == '__main__':
	reload(sys) 
	sys.setdefaultencoding('UTF-8')
	signal.signal(signal.SIGINT, handler)
	signal.signal(signal.SIGTERM, handler)
	rlock = threading.RLock()
	url = None
	depth = 1
	logFile = 'spider.log'
	level = 1
	threadNum = 1
	dbFile = 'spider.db'
	key = '中国'  

	optlist, args = getopt.getopt(
		sys.argv[1:],
		'u:d:f:l:h',
		['thread=', 'dbfile=', 'key='])
	for k, v in optlist:
		if k == '-u':
			url = v
		elif k == '-d':
			depth = int(v)
		elif k == '-f':
			logFile = v
		elif k == '-l':
			level = int(v)
		elif k == '--thread':
			threadNum = int(v)
		elif k == '--dbfile':
			dbFile = v
		elif k == '--key':
			key = v
		elif k == '-h':
			_usage()
			exit()

	if (not 0 < level < 6) or (depth < 1) or (threadNum < -100) or (not url):
		print 'input error'
		_usage()
		exit()
	db, c = _initDB(dbFile)
	logLevel = {
		1: logging.CRITICAL,
		2: logging.ERROR,
		3: logging.WARNING,
		4: logging.INFO,
		5: logging.DEBUG,
	}
	logging.basicConfig(filename=logFile, level=logLevel[level],
						format='%(asctime)s-%(levelname)s-%(filename)s-'
						'%(threadName)s-%(message)s', datefmt='[%d/%b/%Y %H:%M:%S]',)
	depth -= 1
	URLS = set()  
	fileMD5 = set()  
	testself = False
	mainHandler(threadNum, url, depth, key, testself)