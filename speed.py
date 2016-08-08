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


def getseller():
    conn=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebaydata')
    cur=conn.cursor()
    query="select store from cy where cat='fur001'"
    # query="select substore from sellername where store in (select store from sellers)"
    cur.execute(query)
    for store in cur.fetchall():
        for per in monthrange(1,4):
            arg=(store[0],per[0]+'T15:02:52.000Z',per[1]+'T15:02:52.000Z')
            sellerqueue.put(arg)
    conn.close()


def getitem():
    print 'gettime process starts'
    getseller()
    mylist=GetList()
    while True:
        arg=sellerqueue.get()
        items=mylist.get_list(arg[0],arg[1],arg[2])
        if items:
            for item in items:
                print 'Produce item:',item
                itemqueue.put(item)

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
        qury='select itemid from fur001 where itemid=%s'
        sqlinsert='insert into fur001 values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
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
def main():
    ts=time.time()
    getseller()
    gm=multiprocessing.Process(target=getitem,args=())
    handlthread=[]
    for i in range(5):
        phandle=multiprocessing.Process(target=handle,args=())
        handlthread.append(phandle)
    up=multiprocessing.Process(target=Putin().putin,args=())
    gm.start()
    for phandle in handlthread:
        phandle.start()
    up.start()

    for hand in handlthread:
        hand.join()
    gm.join()
    up.join()
    print('Took {}'.format(time.time()-ts))

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

    # main()
    single()
