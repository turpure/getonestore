#! usr/bin/env/python
# -*-coding:utf8--*-

import MySQLdb
import datetime
from multiprocessing.dummy import Pool as ThreadPool
from ebaysdk.trading import Connection


def update_items(item_details):
	query = "update sxb_kw_items set deltasold=%s-quantitysold,quantitysold=%s,deltahit=%s-hitcount,hitcount=%s,deltadays=datediff(now(),curdate),curdate=now() where itemid=%s"
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
	query = "select itemid from sxb_kw_items"
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

def handle(itemid):
	item_details = get_item_details(itemid)
	update_items(item_details)


def main():
	pool = ThreadPool(20)
	items_list_generator = get_item_from_db
	pool.map(handle,items_list_generator())
	pool.close()
	pool.join()


if __name__ == "__main__":
	main()



