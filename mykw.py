__author__ = 'james'

__author__ = 'james'
from twisteditem import GetFiledsByItemid
import datetime
import multiprocessing
from ebaydate import monthrange
from getlist import GetList
import MySQLdb
itemqueue=multiprocessing.Queue()
savequeue=multiprocessing.Queue()
sellerqueue=multiprocessing.Queue()


def getitem(owner):
    qury = "select distinct itemid from %s_category_items where datediff(curdate,startttime)<=60;" % owner
    try:
        con = MySQLdb.connect(host='192.168.0.134',user='root',passwd='',db='ebaydata')
        cur = con.cursor()
        cur.execute(qury)
        for item in cur.fetchall():
            itemqueue.put(item[0])
    except Exception as e:
        print e


def handle():
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
            print 'hanled:%s'% e

class Putin(object):

    conn=MySQLdb.connect(host='192.168.0.134',user='root',passwd='',db='ebaydata')
    cur=conn.cursor()
    def putin(self,owner):
        print 'putin process starts'
        qury='select itemid from ' + owner +'_kw_items where itemid=%s'
        sqlinsert='insert into ' + owner +'_kw_items values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        update_query = 'update ' + owner + '_kw_items set deltasold=%s-quantitysold,quantitysold=%s,deltahit=%s-hitcount,hitcount=%s,deltaday=datediff(curedate,%s),curedate=now()'
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
                                                           l['deltatitle'],l['deltasold'],l['deltahit'],l['deltaprice'],l['listingstatus'],0
                                                           ))
                        self.conn.commit()
                        print '%s putting %s into the database' % (datetime.datetime.now(), l["itemid"])
                    else:print '%s already exites' % l['itemid']
                else:print 'putin process is waiting'
            except Exception as e:
                print 'putin:%s' % e

def single(owner):
    getitem(owner)
    handleList=list()
    pn=multiprocessing.Process(target=Putin().putin,args=(owner,))
    for i in range(60):
        phandle=multiprocessing.Process(target=handle,args=())
        handleList.append(phandle)
    for pro in handleList:
        pro.start()
        print "process starting...."
    pn.start()
if __name__=="__main__":

    # main()
    single('sxz')
