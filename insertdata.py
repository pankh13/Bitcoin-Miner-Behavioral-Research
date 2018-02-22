import requests
import pymysql
import time
from threading import Thread
import re


# this function aims to scrap the miner for this block
url_relay = 'https://blockchain.info/block/'
def scrap_relay(hash):
	text = requests.get(url_relay+hash).text
	pattern1 = '<td>Relayed By</td>'  + '\n\s*' + \
		'<td><a href=\"/blocks/(.*)\">'
	#pattern2 = '(^\d.*)BTC</span></td>'
	pattern2 = '<td>Estimated Transaction Volume</td>'  + '\n\s*' + \
		'<td><span data-c=.* data-time=.*>(\d.*) BTC'	
	pattern3 = '<td>Output Total</td>'  + '\n\s*' + \
		'<td><span data-c=.* data-time=.*>(\d.*) BTC'
	return [re.findall(pattern1,text)[0],round(float(re.findall(pattern2,text)[0].replace(',','')),6),round(float(re.findall(pattern3,text)[0].replace(',','')),6)]
	
def get_data(n):
	url = "https://blockchain.info/it/block-height/" + str(n) + "?format=json"
	response = requests.get(url)
	apidata = response.json()
	blockdata = apidata["blocks"][0]
	del blockdata["tx"]
	blockdata.update(zip(['relayed_by','amount','total_output'],scrap_relay(blockdata['hash'])))
	#blockdata['relayed_by'] = scrap_relay(blockdata['hash'])[0]
	#blockdata['amount'] = scrap_relay(blockdata['hash'])[1]
	return blockdata	
	
def insert_block_data(data):
	# connect to the PostgreSQL database
	conn = psycopg2.connect('dbname=block user=postgres password=postgres')
	# create a new cursor
	cur = conn.cursor()   
	# prepare the INSERT statement
	str_vars = '('+', '.join(list(data.keys()))+')'
	str_values = '('+','.join(str(int(e)) if type(e) != str else str('\''+e+'\'') for e in list(data.values()))+')'
	sql = "INSERT INTO blocks %s VALUES %s"
	sql = sql %(str_vars, str_values)
#	print(sql)
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
		except:
			count +=1
			if count > 20:
				print(i)
				print('data wrong')
				return('fail')
			continue
#	insert_block_data(data)
	try:
		insert_block_data(data)
		return('success')
	except:
		print(i)
		print('insert wrong')
		return('fail')
#insert(193133)	
#when there're too many threads Python is overflowed; if too few then too slow
#now the excution time is of linear complexity
start = 0
end = 492301
gap = 250

threadlist=[]
for i in range(start, end):
	if i-start >gap:
		threadlist[i-start-gap].join()
		if not t.isAlive():
			t.handled = True
	t = Thread(target = lambda: insert(i))
	t.start()
	threadlist.append(t)

for t in range(end-gap,end):
	threadlist[t].join()

	
'''
for size in range(374905,491930,300):
	threadlist = []
	print(size)
	for i in range(300):
		t = Thread(target = lambda: insert(i+size))
		t.start()
		threadlist.append(t)
		
	for t in threadlist:
		t.join()
		if not t.isAlive():
        # get results from thtead
			t.handled = True
print('complete')
'''