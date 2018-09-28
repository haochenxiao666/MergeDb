#!/usr/bin/python
#coding=utf-8

import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import datetime,time
import collections
import logging
import ConfigParser
import re

import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from pymysqlreplication.row_event import (
DeleteRowsEvent,
UpdateRowsEvent,
WriteRowsEvent,
)


#########################define log##############################
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
logger = logging.getLogger("muti")
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
file_handler = logging.FileHandler(BASE_DIR + "/binlog.log")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

##[initialization message]

config = ConfigParser.ConfigParser()
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
config.read(os.path.join(BASE_DIR, 'binlog.conf'))

#[mysql]
my_user = config.get('mysql','user')
my_passwd = config.get('mysql','passwd')
my_host = config.get('mysql','host')
my_port = config.get('mysql','port')
my_charset = config.get('mysql','charset')
my_unix_socket = config.get('mysql','unix_socket')
my_server_id = config.getint('mysql','server_id')


#[tidb]
ti_user = config.get('tidb','user')
ti_passwd = config.get('tidb','passwd')
ti_host = config.get('tidb','host')
ti_port = config.getint('tidb','port')
ti_charset = config.get('tidb','charset')
ti_db = config.get('tidb','db')

#db route-rules

onlyschema = config.get('route-rules','only_schema')
only_schema = onlyschema.split(',')

routeschema = config.get('route-rules','route_schema')
route_schema = routeschema.split(',')

DBR = {}
for n in range(0,len(only_schema)):
	DBR[only_schema[n]] = route_schema[n]

if DBR == {}:
	print 'The route of db is bad,it will be exists!'
	sys.exit()

	
	
	
#connect to tidb
db=pymysql.connect(user=ti_user,passwd=ti_passwd,host=ti_host,port=ti_port,db=ti_db,charset=ti_charset)
con=db.cursor()

#setting for mysql
MYSQL_SETTINGS = {
	"host": my_host,
	"port": my_port,
	"user": my_user,
	"passwd": my_passwd,
	'charset': my_charset,
	#默认无需添加
	'unix_socket': my_unix_socket
}

def compare_items(items):
	# caution: if v is NULL, may need to process
	(k, v) = items
	if v is None:
		return '`%s` = NULL' % k
	else:
		return "`%s`='%s'" % (k,v)
		
def savepos(file,pos):
	with open('binlogpos.meta','w') as g:
		savemeta = {'file':file,'pos':pos}
		g.write(json.dumps(savemeta))	
		
def TransFer(binlogevent_schema,bdb,template,stream_log_file,stream_log_pos):
	try:
		con.execute(template)
		db.commit()
		#记录日志位置
		savepos(stream_log_file,stream_log_pos)
		logger.info("source %s,route %s, %s ,当前读取binlog文件是%s, 读取位置是%s,执行的sql是 %s"%(binlogevent_schema,bdb,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),\
		stream_log_file,stream_log_pos,template))
	except:
		try:
			time.sleep(10)
			con.execute(template)
			db.commit()
			#记录日志位置
			savepos(stream_log_file,stream_log_pos)
			logger.info("source %s,route %s, %s ,当前读取binlog文件是%s, 读取位置是%s,执行的sql是 %s"%(binlogevent_schema,bdb,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),\
			stream_log_file,stream_log_pos,template))
			
		except:	
			savepos(stream_log_file,stream_log_pos)
			logger.info("source %s,route %s, %s ,当前读取binlog文件是%s, 读取位置是%s,执行发生异常的sql是 %s"%(binlogevent_schema,bdb,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),\
			stream_log_file,stream_log_pos,template))
			sys.exit()
	
	
def GetColumnDict(column_dict):
	query = """
			select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION 
			from information_schema.COLUMNS 
			where TABLE_SCHEMA = '%s' 
			order by TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION;"""%ti_db
			
	con.execute(query)
	numrows = int(con.rowcount)
	
	for i in range(numrows):
		row = con.fetchone()
		database = row[0]
		table = row[1]
		column = row[2]
		order = row[3]
		if database in column_dict:
			if table in column_dict[database]:
				column_dict[database][table].update({ order : column })
			else:
				column_dict[database].update( { table : { order : column } })
		else:
			column_dict.update({ database : { table : { order : column } } })

			
#tidb列信息
column_dict = {}
GetColumnDict(column_dict)
#所有表信息
alltidbtables = column_dict[ti_db].keys()
#添加异构有序字段
addcolumn = config.items('addfield')
S = collections.OrderedDict()
for clm in addcolumn:
	S[clm[0]] = clm[1]	



def main():

	#读取binlog 位置
	#默认位置
	print 'start'
	#log_pos = 51487324
	#log_file="mysql-bin.000008"
	
	
	blposfile = 'binlogpos.meta'	

	if os.path.exists(blposfile):
		with open(blposfile) as f:
			log_message = f.readline()
			binlogmessage = json.loads(log_message)
			log_file = binlogmessage['file']
			log_pos = binlogmessage['pos']
	elif os.path.exists('syncer.meta'):
		smpos = open('syncer.meta','r').readlines()
		log_file = ((smpos[0].split('=')[1]).split('"')[1]).strip()
		log_pos = (smpos[1].split('=')[1]).strip()
	else:
		print 'binlog 文件不存在,退出！'
		sys.exit()
	
	
	stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
								server_id=my_server_id,
								resume_stream=True,
								blocking=True,
								freeze_schema=True,
								only_schemas=only_schema,
								only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
								log_file=log_file,
								log_pos=log_pos
								)

	#flag=stream.log_pos	

	for binlogevent in stream:

		#映射
		bdb = DBR[binlogevent.schema]
		
		#过滤
		if binlogevent.table in alltidbtables:
			pass
		else:
			continue
			
		for row in binlogevent.rows:
			
			if isinstance(binlogevent,WriteRowsEvent):
			
				#异构
				for v in S:
					row['values'][v] = S[v]
			
			
				#去除多余字段
				tidblist = column_dict[ti_db][binlogevent.table].values()
				mylist = row['values'].keys()
				for m in mylist:
					if m not in tidblist:
						del row['values'][m]
				
				#判断是否有双引号,解决双引号异常		
				for va in row['values']:
					if isinstance(row['values'][va],unicode):
						if row['values'][va].find("'") >0:
							if row['values'][va].find(r"\'") >0:
								pass
							else:
								row['values'][va] = row['values'][va].replace("'",r"\'")
								print row['values'][va]
				
				template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
					bdb,binlogevent.table,
					', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
					', '.join(map(lambda v: "'%s'" % v,row["values"].values()))
				)
				
				TransFer(binlogevent.schema,bdb,template,stream.log_file,stream.log_pos)
				
			
			elif isinstance(binlogevent, DeleteRowsEvent):
				print 'This is DELETE OPTIONS'			
				#异构
				for v in S:
					row['values'][v] = S[v]
					
				#判断是否有双引号,解决双引号异常		
				for va in row['values']:
					if isinstance(row['values'][va],unicode):

						s = row['values'][va]
						if s.find('"') >0:
							if s.find(r'\"') >0:
								pass
							else:
								row['values'][va] = s.replace('"',r'\"')
								print row['values'][va]
								
						if len(re.findall("\'",s)) == 1:
							row['values'][va] = s.replace("\'","")
							print row['values'][va]
						
				
				
				#去除多余字段
				tidblist = column_dict[ti_db][binlogevent.table].values()
				mylist = row['values'].keys()
				for m in mylist:
					if m not in tidblist:
						del row['values'][m]
				
				
				template = 'DELETE FROM `{0}`.`{1}` WHERE {2} ;'.format(
					bdb, binlogevent.table, ' AND '.join(map(compare_items, row['values'].items()))
				)
				
				template = template.replace('= NULL','IS NULL')
				
				TransFer(binlogevent.schema,bdb,template,stream.log_file,stream.log_pos)
				
			elif isinstance(binlogevent, UpdateRowsEvent):
				print 'This is UPDATE OPTIONS'
				#注入添加字段和值
				for v in S:
					row['before_values'][v] = S[v]
					
				#去除多余字段
				tidblist = column_dict[ti_db][binlogevent.table].values()
				mylist = row['before_values'].keys()
				for m in mylist:
					if m not in tidblist:
						del row['before_values'][m]
						del row['after_values'][m]
		
				#判断是否有双引号,解决双引号异常				
				for v1 in row:
					for va in row[v1]:
						if isinstance(row[v1][va],unicode):
							
							s = row[v1][va]
							if s.find('"') >0:
								if s.find(r'\"') >0:
									pass
								else:
									row[v1][va] = s.replace('"',r'\"')
									print row[v1][va]
									
							if len(re.findall("\'",s)) == 1:
								row[v1][va] = s.replace("\'","")
								print row[v1][va]
				
				template='UPDATE `{0}`.`{1}` set {2} WHERE {3} ;'.format(
					bdb, binlogevent.table,','.join(map(compare_items,row["after_values"].items())),
					' AND '.join(map(compare_items,row["before_values"].items())).replace('= NULL','IS NULL'),datetime.datetime.fromtimestamp(binlogevent.timestamp))
				template = template.replace('""','"')
				
				TransFer(binlogevent.schema,bdb,template,stream.log_file,stream.log_pos)
								
	stream.close()

if __name__ == "__main__":
	main()
	db.close()
