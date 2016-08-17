import os
import sqlite3
class DATABASE:
	def __init__(self, file,logging):
		# init database file, create table(if necessary)
		
		exist = False
		files = os.listdir('.')
		if file in files:
			exist = True
		self.db = sqlite3.connect(file, check_same_thread=False)
		self.c = self.db.cursor()
		self.log = logging
		if not exist:
			try:
				self.c.execute(
					'create table spider(id integer primary key,url text,key text,content text)')
				self.db.commit()
			except sqlite3.OperationalError:
				self.log.cratical(database + 'Error in creating table')

	def insert(self, url, key_word, content):
	# Description : insert data to database
	# 
		try:
			self.c.execute('insert into spider(url,key,content) values("'
					  + url + '","' + key_word + '","' + content + '")')
			self.db.commit()
		except Exception as e:
			self.log.critical(' insert [' + url + '] error.' + str(e))

	def count(self):
		self.c.execute('select count(*) from spider')
		total_num=self.c.fetchall()[0][0]
		return total_num

	def execute(self,cmd):
		try:
			self.c.execute(cmd)
			self.db.commit()
			return self.c.fetchall()
		except Exception as e:
			self.log.critical(' execute [' + cmd + '] error.' + str(e)) 
			return ''