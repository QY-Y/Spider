#!/usr/bin/env
# -*- coding: utf-8 -*-
# __author__ = 'QY-Y'
"""Spider.

Usage:
  spider.py [-u <URL>]
            [<Keyword> <Keyword>...]
            [-d <Depth>]
            [--testself]
            [-f <LogFile>]
            [-l <Level>]
            [--thread <ThreadNumber>]
            [--dbfile <DatabaseFileName>]
  spider.py (-h | --help)
  spider.py --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  -d <Depth>    Depth of the spider.                                [default: 1].
  --testself    Test module                                         [default: False]
  -f <LogFile>  Name/Path of the log file.                          [default: spider.log]
  -l <Level>    How detailed the logging should be(1/less-5/more).  [default: 1]
  --thread <ThreadNumber>                                           [default: 10]
  --dbfile <DatabaseFileName>                                       [default: spider.db]

Description:
  -u <URL> Root URL.
  <Keyword> Keyword list, split by ' '.
"""

import os
import time
import sys
import Queue
import re
from docopt import docopt
import logging
import threading
import signal
import sqlite3
import urllib2
import lxml.html
import hashlib
import gzip
from StringIO import StringIO
from progressbar import *


IS_EXIT = False


def handler(signum, frame):
    # Description : chang global variable IS_EXIT.
    global IS_EXIT
    IS_EXIT = True


def request_url(url):
# Description : send request to url,return contentof response.
# Output :
#     req.get_type()
#     req.get_host()
#     content
    req = urllib2.Request(url)
    req.add_header(
        'User-Agent',
        'Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/48.0')
    req.add_header('Accept-Encoding', 'gzip')
    req.add_header('Accept-Language', 'en-US,en;q=0.5')
    try:
        res = urllib2.urlopen(url, timeout=10)
        content = res.read()
        if res.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(content)
            content = gzip.GzipFile(fileobj=buf).read()
        return req.get_type(), req.get_host(), content
    except Exception as e:
        logging.warning(' open[' + url + '] failed ' + str(e))
        return req.get_type(), req.get_host(), False


def init_data_base(database):
# Description : init database file create table(if necessary)
# Input:
#     dbFile: Name of the Database file
# Output:
#     db: A sqlite3 connection obejct
#     c: A cursor from db
    exist = False
    files = os.listdir('.')
    if database in files:
        exist = True
    db = sqlite3.connect(database, check_same_thread=False)
    c = db.cursor()
    if not exist:
        try:
            c.execute(
                'create table spider(id integer primary key,url text,key text,content text)')
            db.commit()
        except sqlite3.OperationalError:
            logging.cratical(database + 'Error in creating table')
    return db, c


def insert_data(url, key_word, content, saved_count):
# Description : insert data to database
    content = urllib2.quote(str(content))
    try:
        c.execute('insert into spider(url,key,content) values("'
                  + url + '","' + key_word + '","' + content + '")')
        db.commit()
        saved_count.put(url)
    except sqlite3.OperationalError:
        logging.critical(' insert [' + url + '] error')


def argv_check(argvs):
# Description : check user input, exit if illegal
    if (not 0 < argvs['-l'] < 6):
        print '-l must be in [1,2,3,4,5]'
        sys.exit()
    elif argvs['-d'] < 1:
        print '-d must larger than 0'
        sys.exit()
    elif argvs['--thread'] < 1:
        print '--thread must larger than 0'
        sys.exit()
    elif not argvs['-u']:
        print 'Please input root url as -u'
        sys.exit()


# Description : WORKER: search and save pages
class WORKER(threading.Thread):

    def __init__(self, links, keys, rlock, url_set, md5_set, saved_count):
        threading.Thread.__init__(self)
        self.task_queue=links
        self.key_list=keys
        self.rlock=rlock
        self.link=None
        self.depth=None
        self.setDaemon(True)
        self.start()
        self.url_set=url_set
        self.md5_set=md5_set
        self.count=0
        self.saved_count=saved_count

#    Description : 1.get task from task queue.
#                   2.get sub links
#                   3.save pages
    def run(self):
        while True:
            try:
                self.link, self.depth=self.task_queue.get(timeout=2)
            except Exception as e:
                logging.info("get task error")
                break
            try:
                res_host, res_type, data=request_url(self.link)
                if not data:
                    self.task_queue.task_done()
                    continue
                self.count += 1
                md5=hashlib.md5(data).hexdigest()
                if self.depth > 0:
                    self.depth -= 1
                    sub_link_list=self.getLinks(data, res_host, res_type)
                else:
                    sub_link_list=[]
                for sub_link in sub_link_list:
                    if not (sub_link[0] in self.url_set):
                        self.url_set.add((sub_link[0]))
                        self.task_queue.put(sub_link)
                if not (md5 in self.md5_set):
                    self.save(data)
                    self.md5_set.add(md5)
            except Exception as e:
                logging.critical(str(e))
            self.task_queue.task_done()

#    Description : save pages if key word found
    def save(self, data):
        if not data:
            return
        for key in self.key_list:
            if data.find(key) > 0:
                logging.info(" [" + key + "] found in [" + self.link + "]")
                self.rlock.acquire()
                insert_data(self.link, key, data, self.saved_count)
                self.rlock.release()
                break
#    Description : get sub links,returns a list

    def getLinks(self, data, res_type, res_host):
        if not data:
            return []
        host=res_type + '://' + res_host
        try:
            data=data.decode('utf8', 'ignore')
            doc=lxml.html.document_fromstring(data)
        except Exception as e:
            logging.critical(str(e))
        tags=['a', 'iframe', 'frame']
        doc.make_links_absolute(host)
        links=doc.iterlinks()
        new_link_list=[]
        absolute_Link_list=[]
        for l in links:
            if l[0].tag in tags:
                new_link_list.append(l)
        del links
        for link in new_link_list:
            if re.match('http', link[2]):
                absolute_Link_list.append((link[2], self.depth))
        return absolute_Link_list

# Description : manage the task queue and the thread pool,meanwhile show
# progress


class THREAD_POOL:

    def __init__(self, num, event, url, depth, key_word_list):
        self.num=num
        self.event=event
        self.threads=[]
        self.task_queue=Queue.Queue()
        self.task_queue.put((url, depth))
        self.key_list=key_word_list
        self.url_set=set()
        self.url_set.add(url)
        self.md5_set=set()
        self.saved_count=Queue.Queue()
        for i in range(self.num):
            new_thread=WORKER(
                self.task_queue,
                self.key_list,
                rlock,
                self.url_set,
                self.md5_set,
                self.saved_count)
            self.threads.append(new_thread)
        logging.info(" pool init done, " + str(self.num) + " woreker created")
# Description : show progress every 0.5 second and check the global variable IS_EXIT
#           return if IS_EXIT is true or all tasks is done.

    def show_percent(self):
        global IS_EXIT
        while self.task_queue.unfinished_tasks:
            if IS_EXIT:
                try:
                    self.task_queue.empty()
                    self.event.set()
                except Exception as e:
                    logging.critical(str(e))
                return
            width=50
            now=self.task_queue.qsize()
            total=len(self.url_set)
            percent=(float(now) / float(total))
            percent=int((1 - percent) * 100)
            info=('[%%-%ds' % width) % (width * percent / 100 * '=')
            info += '] ' + str(percent) + "%    "
            info += str(len(self.url_set) - self.task_queue.qsize())
            info += '/' + str(len(self.url_set)) + '\r'
            sys.stdout.write((len(info) + 5) * ' ' + '\r')
            sys.stdout.flush()
            sys.stdout.write(info)
            sys.stdout.flush()

            time.sleep(0.5)

        sys.stdout.write('\n')
        print self.saved_count.qsize(), "pages saved. All tasks done at", time.ctime()
        self.event.set()
        return


class testSameDB(threading.Thread):

    def __init__(self, cursor, md5, progress):
        threading.Thread.__init__(self)
        self.c=cursor
        self.count=0
        self.md5=md5
        self.progress=progress
        self.start()

    def run(self):
        while True:
                # get 10,000 contents each time
            self.c.execute('select content from spider limit %s,%s'
                           % (self.count, self.count + 10000))
            self.count += 10000
            contents=self.c.fetchall()
            if len(contents) == 0:
                break
            for c in contents:
                res=hashlib.md5(c[0].encode('utf8'))
                self.md5.add(res.hexdigest())
                self.progress[0]=self.count


# checke if there are duplicate pages in database(by MD5)
def test(dbFile):
    progress=[0]
    md5=set()
    # get count of all pages in database.
    db=sqlite3.connect(dbFile, check_same_thread=False)
    c=db.cursor()
    c.execute('select count(*) from spider')
    total_num=c.fetchall()[0][0]
    if total_num is 0:
        logging.warning("0 content in database")
        return
    t=testSameDB(c, md5, progress)
    t.join()
    pBar=ProgressBar(widgets=[Percentage(), Bar()],
                       maxval=total_num).start()
    while progress[0] < total_num:
        pBar.update(progress[0] + 1)
    pBar.finish()
    if len(md5) == total_num:
        print('no duplicate pages found in database')
    else:
        print('duplicate pages found in database')


def main_handler(thread_num, url, depth, key_word_list, is_test):
    event=threading.Event()
    event.clear()
    pool=THREAD_POOL(
        num=thread_num,
        event=event,
        url=url,
        depth=depth,
        key_word_list=key_word_list)
    pool.show_percent()
    if is_test:
        test(dbFile)


if __name__ == '__main__':
    arguments=docopt(__doc__, version='0.1')
    try:
        arguments['-l']=int(arguments['-l'])
        arguments['-d']=int(arguments['-d'])
        arguments['--thread']=int(arguments['--thread'])
        
    except:
        print "-l,-d,--thread must be numbers"
        sys.exit()
    argv_check(arguments)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    rlock=threading.RLock()

    db, c=init_data_base(arguments['--dbfile'])
    logLevel={
        1: logging.CRITICAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG,
    }
    logging.basicConfig(
        filename=arguments['-f'],
        level=arguments['-l'],
        format='%(asctime)s-%(levelname)s-%(filename)s-'
        '%(threadName)s-%(message)s',
        datefmt='[%d/%b/%Y %H:%M:%S]',
    )
    main_handler(
        arguments['--thread'],
        arguments['-u'],
        arguments['-d'] - 1,
        arguments['<Keyword>'],
        arguments['--testself'])