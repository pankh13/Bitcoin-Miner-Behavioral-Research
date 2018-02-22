import requests
import pymysql
import time
from threading import Thread
import re
import pandas as pd

url_relay = 'https://blockchain.info/block/'
def scrap_relay(hash):
	try:
		response = requests.get(url_relay+hash)
	except:
		print('connection 2 error')
	text = response.text
	pattern1 = '<td>Relayed By</td>'  + '\n\s*' + \
		'<td><a href=\"/blocks/(.*)\">'
	#pattern2 = '(^\d.*)BTC</span></td>'
	pattern2 = '<td>Estimated Transaction Volume</td>'  + '\n\s*' + \
		'<td><span data-c=.* data-time=.*>(\d.*) BTC'
	try:
		a = re.findall(pattern1,text)[0]
	except:
		print('no relay' + hash)
	try:
		a= round(float(re.findall(pattern2,text)[0].replace(',','')),4)
	except:
		print('no amount'+hash)		
		
	return [re.findall(pattern1,text)[0],round(float(re.findall(pattern2,text)[0].replace(',','')),4)]
	
def get_data(n):
	url = "https://blockchain.info/it/block-height/" + str(n) + "?format=json"
	try:
		response = requests.get(url)
	except:
		print('connetion 1 fail')
	apidata = response.json()
	blockdata = apidata["blocks"][0]
	del blockdata["tx"]
	blockdata.update(zip(['relayed_by','amount'],scrap_relay(blockdata['hash'])))
	#blockdata['relayed_by'] = scrap_relay(blockdata['hash'])[0]
	#blockdata['amount'] = scrap_relay(blockdata['hash'])[1]
	return blockdata	
	
def insert_block_data(data):
	# connect to the PostgreSQL database
	conn =pymysql.connect(user='ericpan', \
		password='950519', \
		host= '127.0.0.1', \
		port=3306, \
		db='block')
	# create a new cursor
	cur = conn.cursor()   
	# prepare the INSERT statement
	str_vars = '('+', '.join(list(data.keys()))+')'
	str_values = '('+','.join(str(int(e)) if type(e) != str else str('\''+e.replace('\'',' ')+'\'') for e in list(data.values()))+')'
	sql = "INSERT INTO blocks %s VALUES %s"
	sql = sql %(str_vars, str_values)
	print(sql)
	cur.execute(sql)
	conn.commit()
	cur.close()
	conn.close()
def	insert(i):
	data = None
	count =0
	while(data == None):
		try:
			data = get_data(i)
			print('success'+i)
		except:
			count +=1
			if count > 20:
				print(i)
				print('data wrong')
				return('fail')
			continue
	insert_block_data(data)
	try:
		insert_block_data(data)
		return('success')
	except:
		print(i)
		print('insert wrong')
		return('fail')
	
conn =pymysql.connect(user='ericpan', \
	password='950519', \
	host= '127.0.0.1', \
	port=3306, \
	db='block')
df = pd.read_sql_query("select height from blocks", con = conn).sort_values(by="height")
height = set(df.height)
height_check = set(range(400001))
ms = height ^ height_check
print(ms)
miss = [x for x in ms]
len(miss)

gap = 200
threadlist = []
for i in range(0, len(miss)):
	if len(threadlist) > gap:
		threadlist[len(threadlist)-gap].join()
		if not t.isAlive():
			t.handled = True
	t = Thread(target = lambda: insert(miss[i]))
	t.start()
	threadlist.append(t)

for t in range(0,len(miss)):
	try:
		threadlist[t].join()
	except:
		continue