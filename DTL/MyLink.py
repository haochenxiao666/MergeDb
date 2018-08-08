#encoding:utf-8
#name:mod_db.py

from __future__ import division
import MySQLdb
import MySQLdb.cursors
import mod_config
#import mod_logger

DB = "database"
DBNAME = mod_config.getConfig(DB, 'dbname')
DBHOST = mod_config.getConfig(DB, 'dbhost')
DBUSER = mod_config.getConfig(DB, 'dbuser')
DBPWD = mod_config.getConfig(DB, 'dbpassword')
DBCHARSET = mod_config.getConfig(DB, 'dbcharset')
DBPORT = mod_config.getConfig(DB, "dbport")

#logger = mod_logger.logger(LOGPATH)

class DataBase():
	def __init__(self, dbname=None, dbhost=None):
		#self._logger = logger
		if dbname is None:
			self._dbname = DBNAME
		else:
			self._dbname = dbname
		if dbhost is None:
			self._dbhost = DBHOST
		else:
			self._dbhost = dbhost
			
		self._dbuser = DBUSER
		self._dbpassword = DBPWD
		self._dbcharset = DBCHARSET
		self._dbport = int(DBPORT)
		self._conn = self.connectMySQL()
		
		if(self._conn):
			self._cursor = self._conn.cursor()


	#数据库连接
	def connectMySQL(self):
		conn = False
		try:
			conn = MySQLdb.connect(host=self._dbhost,
					user=self._dbuser,
					passwd=self._dbpassword,
					db=self._dbname,
					port=self._dbport,
					cursorclass=MySQLdb.cursors.DictCursor,
					charset=self._dbcharset,
					)
		except Exception,data:
			#self._logger.error("connect database failed, %s" % data)
			conn = False
		return conn


	#获取查询结果集
	
	def fetch_all(self, sql):
		res = ''
		if(self._conn):
			try:
				self._cursor.execute(sql)
				res = self._cursor.fetchall()
			except Exception, data:
				print 'SQL is %s, Error is %s'%(sql,data)
		return res
	
	
	def update_all(self, sql):
		res = ''
		if(self._conn):
			try:
				print sql
				self._cursor.execute(sql)
				self._conn.commit()
				res = self._cursor.fetchall()
				#self.close()
			except Exception, data:
				print 'ErrorSql is %s ,ErrorMessage is %s'%(sql,data)
		return res
	
	def batch_all(self,sql,T):
		res = ''
		if(self._conn):
			try:
				#print sql
				self._cursor.executemany(sql,T)
				self._conn.commit()
				res = self._cursor.fetchall()
			except Exception, data:
				print 'ErrorSql is %s ,ErrorMessage is %s'%(sql,data)
		return res
	

	#关闭数据库连接
	def close(self):
		if(self._conn):
			try:
				self._cursor.close()
				self._conn.close()
			except Exception, data:
				self._cursor.close()
				self._conn.close()
