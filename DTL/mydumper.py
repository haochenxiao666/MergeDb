#!/usr/bin/python
# -*- coding: utf-8 -*
#auth:haochenxiao
#message: 用于把tidb 从库中的数据合并到scm_merge_chain 合并库中;或者也可以从mysql 主库直接查询合并到tidb。
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from xpinyin import Pinyin
from MyLink import *

s = sys.argv[1]
p = Pinyin()
clientname = p.get_pinyin(unicode(s),'')

#此处根据自己规则去定义
dbname = 'scm_%s_chain'%clientname
mergedb = 'scm_merge_db'

db = DataBase(dbname=dbname)
mgdb = DataBase(dbname=mergedb)

#对对照表做哈希
V = {}
hsql = 'select * from scm_client;'
hashresult = mgdb.fetch_all(hsql)
for j in hashresult:
	V[j['ename']] = j['cliid']
#获取客户id	
clientid = V[clientname]
#获取所有表列表
alltblist = []
alltbsql = 'show tables;'
alltb = mgdb.fetch_all(alltbsql)
for i in alltb:
	alltblist.append(i['Tables_in_%s'%mergedb])


def InsertDataRange(m):
	db = DataBase(dbname=dbname)
	mgdb = DataBase(dbname=mergedb)
	#测试数据清理
	#cleansql = 'TRUNCATE table %s'%m
	#mgdb.fetch_all(cleansql)
	#获取表列名称
	schemasql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s' \
				and table_schema = '%s';"%(m,mergedb)
	schemalist = []
	schemaresult = mgdb.fetch_all(schemasql)
	for i in schemaresult:
		schemalist.append(i['COLUMN_NAME'])
	del schemalist[0:3]
	
	#分布查询一个表获取所有数据	
	#获取主键id名称
	pksql = 'desc %s'%m
	pkresult = db.fetch_all(pksql)	
	if pkresult == '':
		print '%s 表结构不存在跳过'%m
		return
		
	pkname = ''
	for k in pkresult:
		if k['Key'] == 'PRI':
			pkname = k['Field']
			break
		else:
			pkname = k['Field']
			break
		
	#查询条数判断是否分批次导入
	lastsql = 'select count(%s) from %s;'%(pkname,m)
	lastv = db.fetch_all(lastsql)
	try:
		lastnum = int(lastv[0]['count(%s)'%pkname])
	except Exception,data:
		print 'error is ',data
		lastnum = 0
	
	print '%s 总条数是%s'%(m,lastnum)
	
	if lastnum == 0:
		print '%s 没有数据,跳过'%m
		return 'jmp'
	
	#设置达到biggest 最大条数后开始分批次导入
	biggest = 500000
	#每次导入最大条数
	mid = 200000
	if lastnum >= biggest:
		print '数据量超过50万,使用批次导入'
		t = (lastnum / mid) + 1
		#获取第一条主键num
		fn = db.fetch_all('select %s from %s limit 1;'%(pkname,m))
		fnum = int(fn[0][pkname])
		#获取最后一条主键num
		lsn = db.fetch_all('select %s from %s order by %s desc limit 1;'%(pkname,m,pkname))
		lnum = int(lsn[0][pkname])
		
		nm = lnum - fnum
		tv = nm / t
	
		for d in range(1,t+2):
		
			tfm = fnum
			fnum += tv	
			
			print tfm
			
			#if fnum >= (lnum + tv):
			#	break
			
			sql = 'select * from %s where %s >= %s and %s < %s;'%(m,pkname,str(tfm),pkname,str(fnum))
			sqlresult = db.fetch_all(sql)
	
			#初始化
			num = 0
			Td = []
			#对列名称加标识
			sclist = [ '`%s`'%x for x in schemalist]
			mc = ','.join(sclist)
			bss = ('%s,'*(len(schemalist) + 1))[:-1]
			mergesql = "insert into %s (`clientid`,%s) values (%s)"%(m,mc,bss)
			
			for i in sqlresult: 					
				#批量插入
				num += 1
				tmp = []
				tmp.append(clientid)
				for j in schemalist:
					if j == 'post' or j == 'formula':
						v = i[j]
						if v != '':
							v = v.replace('\\"','\"')
							v = v.replace('\"','\\"')
						tmp.append(v)
					else:
						try:
							tmp.append(i[j])
						except:
							tmp.append('')
				
				Td.append(tmp)
				if num % 10000 == 0:
					mgdb.batch_all(mergesql,Td)
					Td = []
					
			mgdb.batch_all(mergesql,Td)
	
	else:
		print '数据量比较小,全部查出直接导入'
		sql = 'select * from %s;'%(m)
		print sql
		sqlresult = db.fetch_all(sql)
		
		#初始化
		num = 0
		Td = []
		#对列名称加标识
		sclist = [ '`%s`'%x for x in schemalist]
		mc = ','.join(sclist)
		bss = ('%s,'*(len(schemalist) + 1))[:-1]
		mergesql = "insert into %s (`clientid`,%s) values (%s);"%(m,mc,bss)
				
		for i in sqlresult: 

			#批量插入
			num += 1
			tmp = []
			tmp.append(clientid)
			for j in schemalist:
				if j == 'post'  or j == 'formula':
					v = i[j]
					if v != '':
						v = v.replace('\\"','\"')
						v = v.replace('\"','\\"')
					tmp.append(v)
				else:
					try:
						tmp.append(i[j])
					except:
						tmp.append('')
					
			Td.append(tmp)
			if num % 20000 == 0:
				mgdb.batch_all(mergesql,Td)
				Td = []
						
		mgdb.batch_all(mergesql,Td)
		
#单表插入	
#InsertDataRange('sys_param')

#所有表插入	
for m in alltblist:
	#跳过哈希表
	if m == 'scm_client' or m == 'client':
		continue
	InsertDataRange(m)
	#InsertData(m)

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
