__author__ = 'james'
from getupdatefields import GetUpdateFields
import MySQLdb
import time
import multiprocessing
import datetime
itemqueue=multiprocessing.Queue()
savequeue=multiprocessing.Queue()
def getitem():
    conn=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebaydata')
    cur=conn.cursor()
    query='select itemid from bigseller'
    cur.execute(query)
    item_tuple=cur.fetchall()
    # for item in item_tuple
    for item in item_tuple:
        itemqueue.put(item[0])
    conn.close()

def handle():
    while itemqueue.qsize():
        try:
            id=itemqueue.get()
            myitem=GetUpdateFields()
            xml=myitem.get_xml(id)
            if xml:
                detail=myitem.parse(xml)
                with open('/home/james/ebaydata.log','a') as log:
                        log.write('%s: put one entry in the savequeue\n' % time.ctime())
                print '%s: put one entry in the savequeue\n' % time.ctime()
                savequeue.put(detail)
        except Exception as e:
            with open('/home/james/exception.log','a') as log:
                log.write("%s:error from handle %s\n%s\n" % (time.ctime(),e))

class Update(object):
    conn=MySQLdb.connect(host='192.168.1.160',user='root',
                             passwd='urnothing',db='ebaydata')
    cur=conn.cursor()
    def _upprice(self,l):
        query='select currentprice from bigseller where itemid=%s'
        upgrade='update bigseller set currentprice=%s,deltaprice=%s where itemid=%s'
        self.cur.execute(query,l['itemid'])
        try:
            lastprice=self.cur.fetchone()[0]
            currentprice=l['currentprice']
            deltaprice=float(currentprice)-float(lastprice)
            print "%s:%s price is updated by %s" % (time.ctime(),l['itemid'],str(deltaprice))
            self.cur.execute(upgrade,(currentprice,deltaprice,l['itemid']))
            self.conn.commit()
        except Exception as e:
            print l['itemid'],e
            with open('/home/james/exception.log','a') as log:
                log.write("%s:error from handle %s\n" % (time.ctime(),e))
    def _upsold(self,l):
        query='select quantitysold from bigseller where itemid=%s'
        upgrade='update bigseller set quantitysold=%s,deltasold=%s where itemid=%s'
        self.cur.execute(query,l['itemid'])
        try:
            lastsold=self.cur.fetchone()[0]
            currensold=l['quantitysold']
            deltasold=int(currensold)-int(lastsold)
            print "%s:%s sold updated by %s" % (time.ctime(),l['itemid'],deltasold)
            self.cur.execute(upgrade,(currensold,deltasold,l['itemid']))
            self.conn.commit()
        except Exception as e:
                with open('/home/james/exception.log','a') as log:
                    log.write("%s:error from handle %s\n" % (time.ctime(),e))
    def _uphit(self,l):
        query='select hitcount from bigseller where itemid=%s'
        upgrade='update bigseller set hitcount=%s ,deltahit=%s where itemid=%s'
        self.cur.execute(query,l['itemid'])
        try:
            lasthit=self.cur.fetchone()[0]
            currenthit=l['hitcount']
            deltahit=int(currenthit)-int(lasthit)
            print "%s:%s hit is updated by %s" % (time.ctime(),l['itemid'],deltahit)
            self.cur.execute(upgrade,(currenthit,deltahit,l['itemid']))
            self.conn.commit()
        except Exception as e:
            print e
            with open('/home/james/exception.log','a') as log:
                log.write("%s:error from handle %s\n" % (time.ctime(),e))
    def _upday(self,l):

        query='select curdate from bigseller where itemid=%s'
        upgrade='update bigseller set curdate=%s, deltaday=%s where itemid=%s'
        self.cur.execute(query,(l['itemid']))
        # print cur.fetchone()[0]

        date1=str(self.cur.fetchone()[0])
        date2=str(datetime.datetime.now())[:10]
        lastdate=date1.split('-')
        currentdate=date2.split('-')
        last=datetime.date(int(lastdate[0]),int(lastdate[1]),int(lastdate[2]))
        current=datetime.date(int(currentdate[0]),int(currentdate[1]),int(currentdate[2]))
        delta=current-last
        deltaday=delta.days
        # print deltaday
        try:
            print '%s:%s deltaday updated by %s' % (time.ctime(),l["itemid"],deltaday)
            self.cur.execute(upgrade,(date2,deltaday,l['itemid']))
            self.conn.commit()
        except Exception as e:
                with open('/home/james/exception.log','a') as log:
                    log.write("%s:error from handle %s\n" % (time.ctime(),e))

    def update(self):
        while True:
            try:
                t=savequeue.get()
                self._upprice(t)
                self._upsold(t)
                self._uphit(t)
                self._upday(t)
                # print '%s is updated.....' % t['itemid']
                # with open('/home/james/fastupdate.log','a') as log:
                #     log.write("%s:%s is updated....\n" % (time.ctime(),t['itemid']))
            except Exception as e:
                print e
                with open('/home/james/exception.log','a') as log:
                    log.write("%s:error from handle %s\n" % (time.ctime(),e))
                # break
def main():
    ts=time.time()
    getitem()
    handlthread=[]
    for i in range(30):
        phandle=multiprocessing.Process(target=handle,args=())
        handlthread.append(phandle)
    up=multiprocessing.Process(target=Update().update,args=())
    for phandle in handlthread:
        phandle.start()
    up.start()

    for hand in handlthread:
        hand.join()
    print('Took {}'.format(time.time()-ts))

if __name__=="__main__":
    # getitem()
    # handle()
    # Update().update()
    main()
