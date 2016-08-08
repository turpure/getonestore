__author__ = 'james'
from twisteditem import GetFiledsByItemid
import time
import multiprocessing
from ebaydate import monthrange
from getlist import GetList
import MySQLdb
itemqueue=multiprocessing.Queue()
savequeue=multiprocessing.Queue()
sellerqueue=multiprocessing.Queue()




def getitem():
    print 'gettime process starts'
    conn=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebay_sellers')
    cur=conn.cursor()
    query="select itemid from uscat where cate='Lots'"
    # query="select substore from sellername where store in (select store from sellers)"
    cur.execute(query)
    for item in cur.fetchall():
        itemqueue.put(item[0])
    conn.close()


def handle():
    print 'Handle procces starts'
    while True:
        try:
            id=itemqueue.get()
            myitem=GetFiledsByItemid()
            xml=myitem.get_xml(id)
            if xml:
                detail=myitem.parse(xml)
                # with open('/home/james/ebaydata.log','a') as log:
                #         log.write('%s: put one entry in the savequeue\n' % time.ctime())

                print 'Comsume itemd:',id
                savequeue.put(detail)
        except Exception as e:
            # with open('/home/james/exception.log','a') as log:
            #     log.write("%s:error from handle %s\n%s\n" % (time.ctime(),e))
            print e

class Putin(object):

    conn=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebaydata')
    cur=conn.cursor()
    def putin(self):
        print 'putin process starts'
        qury='select itemid from Lots where itemid=%s'
        sqlinsert='insert into Lots values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        while True:
            try:
                l=savequeue.get()
                if l:
                    if not self.cur.execute(qury,(l['itemid'],)):
                        self.cur.execute(sqlinsert,(l['country'],l['currency'],l['hitcounter'],
                                                           l['itemid'],l['starttime'],l['viewitemurl'],
                                                           l['location'],l['categoryid'],l['categoryname'],
                                                           l['feedbackscore'],l['feedbackstar'],l['usersite'],
                                                           l['userid'],l['storeowner'],l['currentprice'],
                                                           l['quantitysold'],l['quantitysoldinstore'],
                                                           l['shippingservice'],l['shippingcost'],
                                                           l['title'],l['hitcount'],l['sku'],l['galleryurl'],
                                                           l['listduration'],l['privatelisting'],l['curdate'],
                                                           l['deltatitle'],l['deltasold'],l['deltahit'],l['deltaprice'],0,l['listingstatus']
                                                           ))
                        self.conn.commit()
                        print 'putting %s into the database' % l["itemid"]
                    else:print '%s already exites' % l['itemid']
                else:print 'putin process is waiting'
            except Exception as e:
                print e

def single():

    handleList=list()
    gm=multiprocessing.Process(target=getitem,args=())
    pn=multiprocessing.Process(target=Putin().putin,args=())
    for i in range(35):
        phandle=multiprocessing.Process(target=handle,args=())
        handleList.append(phandle)
    # up=multiprocessing.Process(target=Putin().putin,args=())
    gm.start()
    for pro in handleList:
        pro.start()
    pn.start()
if __name__=="__main__":

    single()
    # getitem()
    # single()
