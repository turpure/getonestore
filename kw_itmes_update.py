#! usr/bin/env/python
# -*-coding:utf8--*-

import MySQLdb
import datetime
from multiprocessing.dummy import Pool as ThreadPool
from ebaysdk.trading import Connection
from functools import partial

def update_items(item_details,owner):
	query = "update " + owner + "_kw_items set deltasold=%s-quantitysold,quantitysold=%s,deltahit=%s-hitcount,hitcount=%s,deltadays=datediff(now(),curdate),curdate=now() where itemid=%s"
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


def get_item_from_db(owner):
	query = "select itemid from " + owner + "_kw_items where datediff(curdate,startttime)<30;"
	try:
		con = MySQLdb.connect(host='192.168.0.134',user='root',passwd='',db='ebaydata')
		cur = con.cursor()
		cur.execute(query)
		for item in cur.fetchall():
			yield item[0]
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

def handle(itemid,owner):
	item_details = get_item_details(itemid)
	update_items(item_details,owner)


def main(a_owner):
	partial_handle = partial(handle,owner=a_owner)
	pool = ThreadPool(24)
	pool.map(partial_handle,get_item_from_db(a_owner))
	pool.close()
	pool.join()


if __name__ == "__main__":
	# print get_item_details('401172859387')
	main('test')
	# handle('401172859387','test')
	# print [i for i in get_item_from_db('test')]
	# partial_handle = partial(handle,owner='test')
	# partial_handle('401172859387')


