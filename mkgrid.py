#!/usr/bin/env python
# coding=utf-8

from pyspatialite import dbapi2 as db
#import sqlite3
import sys
import math
import getopt

output_len = 0;
need_update = False

try:
	optlist, args = getopt.getopt(sys.argv[1:],'suf:t:l:b:r:w:h:')
	db_file = filter(lambda item: item[0]=='-f',optlist)[0][1]
	top = float(filter(lambda item: item[0]=='-t',optlist)[0][1])
	left = float(filter(lambda item: item[0]=='-l',optlist)[0][1])
	bottom = float(filter(lambda item: item[0]=='-b',optlist)[0][1])
	right = float(filter(lambda item: item[0]=='-r',optlist)[0][1])
	rows = int(filter(lambda item: item[0]=='-h',optlist)[0][1])
	cols = int(filter(lambda item: item[0]=='-w',optlist)[0][1])
	step_lat = (top - bottom)/rows
	step_lng = (right - left)/cols
	#print 'step_lat=%f step_lng=%f' % ( step_lat, step_lng)
	
except:
	print 'Usage: %s [-s] [-u] -f <db_filename> -t <top> -l <left> -b <bottom> -r <right> -w <num_cols> -h <num_rows>' % sys.argv[0]
	exit(1)
	
help1="""
	Скрипт для создания сетки для ускорения поиска узла а графе, представленном
	как база SQLite. Принимает параметром имя файла базы, верхнюю, левую,нижнюю,
	правую границы, количество столбцов и рядов в сетке  
"""

if '-s' not in map(lambda item: item[0],optlist):
	print help1

if '-u' in map(lambda item: item[0],optlist):
	need_update = True
	
#print progress
def print_progress(message,count,size,index=None):
	progress = float(count)/size*100
	if index != None:
		sys.stdout.write("%s: %d%%  (index=%d)  \r" % (message, progress,index) )
		sys.stdout.flush()
	else:
		sys.stdout.write("%s: %d%%  \r" % (message, progress) )
		sys.stdout.flush()
#подключение к БД
def connect_db(db_file):
	#print db_file
	conn = db.connect(db_file)
	# creating a Cursor
	cur = conn.cursor()
	return conn,cur

#загрузка узлов
def load_nodes(cur):
	sql = 'SELECT count(*) FROM roads_nodes'
	res = cur.execute(sql)
	for row in res:
		size = row[0]
	sql = 'select node_id, Y(geometry) as lat, X(geometry) as lng from roads_nodes'
	res = cur.execute(sql)
	nodes = []
	count = 0
	for row in res:
		count = count + 1
		if count % 10000.0 == 0:
			print_progress('',count,size)
		nodes.append({'node_id':row[0], 'lat': row[1], 'lng':row[2], 'sector': latlng2sector(row[1],row[2])})
	return nodes
	

#вычисление сектора по координатам
def latlng2sector(lat,lng):
	global top, left, step_lat, step_lng, cols
	row = math.floor((top - lat)/step_lat)
	col = math.floor((lng - left)/step_lng)
	sector = row * cols + col
	return sector

def column_exists(cur,table,column):
	sql = "SELECT "+column+" from "+table+" LIMIT 1"
	has_connected = True
	try:
		res = cur.execute(sql)
	except:
		has_connected = False
	else:
		has_connected = True
	return has_connected

def table_exists(cur,table):
	sql = "SELECT * from "+table+" LIMIT 1"
	table_exists = True
	try:
		res = cur.execute(sql)
	except:
		table_exists = False
	else:
		table_exists = True
	return table_exists


#добавление столбца в таблицу
def add_column(cur,table,column):
	sql = 'ALTER TABLE '+table+' ADD COLUMN ' + column + ' INTEGER DEFAULT 0'
	print 'Add column "'+column+'"...'
	try:
		res = cur.execute(sql)
	except:
		print 'column "'+column+'" may be exists, try set it to 0'
		try:
			sql = 'UPDATE '+table+' SET '+column+'=0'
			res = cur.execute(sql)
			print 'Setup done'
		except:
			print 'Error during execution of sql query. Exiting...'
			exit(1)	
	else:
		print 'Done'
		return

#запись данных значения сектора в таблицу узлов
def store_sector(nodes, cur,conn):
	BUFFER_SIZE = 10000
	num_rows = len(nodes)
	try:
		print 'Begin change database...'
		offset = 0
		while offset < num_rows:
			for node in nodes[offset:offset+BUFFER_SIZE]:
				sql = 'UPDATE roads_nodes SET sector=' + str(node['sector']) + ' WHERE node_id=' + str(node['node_id'])
				cur.execute(sql)
			conn.commit()
			offset = offset + BUFFER_SIZE
			if offset % 100000.0 == 0:
				print_progress('Progress: ', offset, num_rows)
		print 'Done'
	except:
		print 'Error during execution of sql query. Exiting...'
		return
	
#создание индекса
def create_index(cur,table,column):
	sql = 'DROP INDEX IF EXISTS ' + column + '_i'
	print 'Dropping index...'
	try:
		cur.execute(sql)
	except:
		print 'Error during execution of sql query. Exiting...'
		exit(1)
	else:
		print 'Done'
	sql = 'CREATE INDEX IF NOT EXISTS '+column + '_i' +' ON ' + table + '(' + column + ')'
	print 'Create index "'+column+'_i "...'
	try:
		cur.execute(sql)
	except:	
		print 'Error during execution of sql query. Exiting...'
		exit(1)	
	else:
		print 'Done'
		return
		
conn, cur = connect_db(db_file)

if not table_exists(cur,'roads_nodes'):
	print 'File "%s" is not roads net database' % db_file
	exit(0)


if need_update:
	add_column(cur,'roads_nodes','sector')
else:
	if column_exists(cur,'roads_nodes','sector'):
		print '%s file already processed' % db_file
		exit(0)
	else:
		add_column(cur,'roads_nodes','sector')
	

print 'Load nodes...'
nodes = load_nodes(cur)
print ' Done'

print 'nodes: %d' % (len(nodes))
if len(nodes) == 0:
	print 'Graph is empty. Exiting...'
	exit(1)

answer = raw_input('Save sector numbers to the database? (y/n)')
if answer.lower()[0] == 'y':
	store_sector(nodes,cur,conn)
	create_index(cur,'roads_nodes', 'sector')

