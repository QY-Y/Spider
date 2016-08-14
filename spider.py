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
import urllib2
import traceback
import curses
import lxml.html
import hashlib
import gzip
from StringIO import StringIO


IS_EXIT = False
def handler(signum, frame):
	global IS_EXIT
	IS_EXIT = True
# Host: www.baidu.com
# User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0
# Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
# Accept-Language: en-US,en;q=0.5
# Accept-Encoding: gzip, deflate, br
# Cookie: BAIDUID=53782338724485C9E25F994B5C4CF241:FG=1; BIDUPSID=53782338724485C9E25F994B5C4CF241; PSTM=1461994803; BD_UPN=133352; H_PS_PSSID=1462_17942_11598_20847_20857_20837_20771; H_PS_645EC=aceedZSUvnyvX0JXU8siUp6p87RynkqgVFCSJRLh5fsr1mAbFYAE6qGyEr4; BD_CK_SAM=1; BD_HOME=0; BDRCVFR[eHt_ClL0b_s]=mk3SLVN4HKm
# Connection: keep-alive
# Upgrade-Insecure-Requests: 1
# Pragma: no-cache
# Cache-Control: no-cache

def _requestData(url):
	req = urllib2.Request(url)
	req.add_header('User-Agent',
		'Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/48.0')
	req.add_header('Accept-Encoding','gzip')
	req.add_header('Accept-Language','en-US,en;q=0.5')
	try:
		res = urllib2.urlopen(url,timeout = 10)
		content = res.read()
		# try:
			# content = content.decode('gb2312','ignore')
		# except:
			# content = content.decode('utf-8', 'ignore')
		if res.info().get('Content-Encoding') == 'gzip':
			buf = StringIO(content)
			content = gzip.GzipFile(fileobj=buf).read()
		return req.get_type(), req.get_host(), content
	except Exception,e: 
		logging.warning(' open['+url+'] failed '+str(e))
		return req.get_type(), req.get_host(), False
	

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

def _insert(url, keyWord, content, savedCount):
	'''insert info to defualt database
	Args:
		url: The url to be saved
		keyWord: Key word
		content: Data
	'''
	content = urllib2.quote(str(content))
	try:
		c.execute('insert into spider(url,key,content) values("'
				  +url+'","'+keyWord+'","'+content+'")')
		db.commit()
		savedCount.put(url)
	except sqlite3.OperationalError:
		logging.critical(' insert ['+url+'] error')


class worker(threading.Thread):
	def __init__(self, links, keys, rlock, urlset, md5set, savedCount):
		threading.Thread.__init__(self)
		self.taskQueue = links
		self.keyList = keys
		self.rlock = rlock
		self.link = None
		self.depth = None
		self.setDaemon(True) 
		self.start()
		self.exc_traceback = ''
		self.urlset = urlset
		self.md5set = md5set
		self.count = 0
		self.savedCount = savedCount

	def run(self):
		while True:
			try:
				self.link, self.depth = self.taskQueue.get(timeout=2)
			except Exception,e:
				logging.info("get task error")
				break
			try:
				reshost, restype, data = _requestData(self.link)
				if not data:
					self.taskQueue.task_done()
					continue
				self.count += 1
				md5 = hashlib.md5(data).hexdigest()
				if self.depth > 0:
					self.depth -= 1
					subLinks = self.getLinks(data, reshost, restype)
				else:
					subLinks = []
				for subLink in subLinks:
					if not (subLink[0] in self.urlset):
						self.urlset.add((subLink[0]))
						self.taskQueue.put(subLink)
				if not (md5 in self.md5set):
					self.save(data)
					self.md5set.add(md5)
			except Exception,e:
				logging.critical(str(e))
			self.taskQueue.task_done()


	def save(self,data):
		if not data:
			return
		for key in self.keyList:
			if data.find(key)>0:
				logging.info(" ["+key+"] found in ["+self.link+"]")
				self.rlock.acquire()
				_insert(self.link, key, data, self.savedCount)
				self.rlock.release()
				break

	def getLinks(self, data, resType, resHost):
		if not data:
			return []
		host = resType+'://'+resHost
		try:
			data = data.decode('utf8', 'ignore')
			doc = lxml.html.document_fromstring(data)
		except Exception,e:
			logging.critical(str(e))
		
		tags = ['a', 'iframe', 'frame']
		doc.make_links_absolute(host)
		links = doc.iterlinks()
		newLinks = []
		absoluteLinks = []
		for l in links:
			if l[0].tag in tags:
				newLinks.append(l)
		del links
		for link in newLinks:
			if re.match('http',link[2]):
				absoluteLinks.append((link[2],self.depth))
		return absoluteLinks


class threadPool:
	def __init__(self, num, event, url, depth, keyWords):
		self.num = num
		self.event = event
		self.threads = []
		self.tasks = Queue.Queue()
		self.tasks.put((url, depth))
		self.key = keyWords
		self.url = set()
		self.url.add(url)
		self.urlMD5 = set()
		self.savedCount = Queue.Queue()
		for i in range(self.num):
			newThread = worker(self.tasks, self.key, rlock, self.url, self.urlMD5, self.savedCount)
			self.threads.append(newThread)
		logging.info(" pool init done, " + str(self.num) + " woreker created")


	def supervisor(self):
		global IS_EXIT
		while self.tasks.unfinished_tasks:
			if IS_EXIT:
				try:
					self.tasks.empty()
					self.event.set()
				except Exception,e:
					logging.critical(str(e))
				return
			width = 50
			now = self.tasks.qsize()
			total = len(self.url)
			percent = (float(now)/float(total))
			percent = int((1 - percent)*100)
			printinfo = ('[%%-%ds' % width) % (width * percent / 100 * '=')
			printinfo += '] ' + str(percent) + "%    "
			printinfo += str(len(self.url) - self.tasks.qsize()) 
			printinfo += '/' + str(len(self.url)) + '\r'
			sys.stdout.write((len(printinfo)+5)*' '+'\r')
  			sys.stdout.flush()
			sys.stdout.write(printinfo)
  			sys.stdout.flush()
			time.sleep(1)

		sys.stdout.write('\n')
		print self.savedCount.qsize(),"pages saved. All tasks done at",time.ctime()
		self.event.set()
		return


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
	pool = threadPool(num = threadNum, event = event, url = url, depth = depth, keyWords = keyWord)
	pool.supervisor()
	

if __name__ == '__main__':
	print time.ctime()
	os.system("rm spider.db spider.log")
	signal.signal(signal.SIGINT, handler)
	signal.signal(signal.SIGTERM, handler)
	rlock = threading.RLock()
	url = None
	depth = 1
	logFile = 'spider.log'
	level = 1
	threadNum = 1
	dbFile = 'spider.db'
	# key = ['中国']
	key = ['HTML5']  

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
	testself = False
	mainHandler(threadNum, url, depth-1, key, testself)