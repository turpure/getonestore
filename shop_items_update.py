#! usr/bin/env/python
# -*-coding:utf8--*-

import MySQLdb
import datetime
import multiprocessing
from ebaysdk.trading import Connection
from functools import partial
items_queue = multiprocessing.Queue()

def update_items(item_details):
	query = "update all_shop_items set deltasold=%s-quantitysold,quantitysold=%s,deltahit=%s-hitcount,hitcount=%s,deltadays=datediff(now(),curdate),curdate=now() where itemid=%s"
	try:
		con = MySQLdb.connect(host='192.168.0.134',user='root',passwd='',db='ebaydata')
		cur = con.cursor()
		if item_details:
			cur.execute(query,(item_details['quantitysold'],item_details['quantitysold'],item_details['hitcount'],item_details['hitcount'],item_details['itemid']))
			print "%s:upadating %s" % (datetime.datetime.now(),item_details['itemid'])
		con.commit()
		cur.close()
		con.close()
	except Exception as e:
		print e 


def get_item_from_db():
	query = "select itemid from all_shop_items where datediff(curdate,startttime)<45;"
	try:
		con = MySQLdb.connect(host='192.168.0.134',user='root',passwd='',db='ebaydata')
		cur = con.cursor()
		cur.execute(query)
		for item in cur.fetchall():
			items_queue.put(item[0])
		cur.close()
		con.close()
	except Exception as e:
		print e 


def get_item_details(itemid):
	api = Connection(config_file="ebay.yaml")
	request = {
		"ItemID":itemid,
		"OutputSelector":"Item",
	}
	item_details = dict()
	try:
		response = api.execute("GetItem",request)
		item_details_tree = response.reply.Item
		item_details["itemid"]=itemid
		item_details["quantitysold"] = item_details_tree.SellingStatus.QuantitySold
		item_details["hitcount"] = item_details_tree.HitCount
		# print item_details
	except Exception as e:
		print e
	return item_details

def handle():
	while True:
		try:
			itemid = items_queue.get()
			item_details = get_item_details(itemid)
			if item_details:
				update_items(item_details)
		except Exception as e:
			print e



def main():
	get_item_from_db()
	handle_list = list()
	for i in range(60):
		handle_process = multiprocessing.Process(target=handle,args=())
		handle_list.append(handle_process)

	for process in handle_list:
		process.start()
		print 'process is startting.....'

if __name__ == "__main__":
	main()
