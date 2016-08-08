__author__ = 'james'
import MySQLdb
from getitem import GetFields
from ebaysdk.exception import ConnectionError
import re
from fields import Fields
from concurrent import futures
import ssl
import requests
from multiprocessing.dummy import Pool
import _mysql_exceptions
import ast
import time
# queue=[]
def gen_list():
    conn=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebaydata')
    cur=conn.cursor()
    getquery='select itemid from listingupdate limit 76310,1000000;'
    cur.execute(getquery)
    itemids=cur.fetchall()
    cur.close()
    conn.close()
    return list(itemids)
# itemids=gen_list()
def update(l):
    conn=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebaydata')
    cur=conn.cursor()
    priceupdate='select currentprice from listingupdate where itemid=%s'
    titleupdate='select title from listingupdate where itemid=%s'
    soldupdate='select quantitysold from listingupdate where itemid=%s'
    hitupdate='select hitcount from listingupdate where itemid=%s'
    dateupdate='update listingupdate set curdate=%s where itemid=%s'
    cur.execute(dateupdate,(l['curdate'],l['itemid']))
    conn.commit()
    cur.execute(priceupdate,l['itemid'])
    priceraw=cur.fetchone()[0]
    if not priceraw==l['currentprice']:
        deltap=str(float(l['currentprice'])-float(priceraw))
        print '%s:the currentprice is updated by %s' % (l['itemid'],deltap)
        cur.execute('update listingupdate set currentprice=%s,deltaprice=%s where itemid=%s',(l['currentprice'],deltap,l['itemid']))
        conn.commit()
    if priceraw==l['currentprice']:
        print '%s:the currentprice is the same' % l['itemid']
        cur.execute('update listingupdate set deltaprice=%s where itemid=%s',('0',l['itemid']))
        conn.commit()
    cur.execute(soldupdate,l['itemid'])
    soldraw=cur.fetchone()[0]
    if not soldraw==l['quantitysold']:

        deltas=str(int(l['quantitysold'])-int(soldraw))
        print '%s:the quantitysold is updated %s' % (l['itemid'],deltas)
        cur.execute('update listingupdate set quantitysold=%s,deltasold=%s where itemid=%s',(l['quantitysold'],deltas,l['itemid']))
        conn.commit()
    if soldraw==l['quantitysold']:
        print '%s:the quantitysold is the same' % l['itemid']
        cur.execute('update listingupdate set deltasold=%s where itemid=%s',('0',l['itemid']))
        conn.commit()
    cur.execute(hitupdate,l['itemid'])
    hitraw=cur.fetchone()[0]
    if not hitraw==l['hitcount']:

        deltah=str(int(l['hitcount'])-int(hitraw))
        print '%s:the hitcount is updated by %s' % (l['itemid'],deltah)
        cur.execute('update listingupdate set hitcount=%s,deltahit=%s where itemid=%s',(l['hitcount'],deltah,l['itemid']))
        conn.commit()
    if hitraw==l['hitcount']:
        print '%s:the hitcount is the same' % l['itemid']
        cur.execute('update listingupdate set deltahit=%s where itemid=%s',('0',l['itemid']))
        conn.commit()
    cur.execute(titleupdate,l['itemid'])
    titleraw=cur.fetchone()[0]
    if not titleraw==l['title']:
        print '%s:the title is updated' % l['itemid']
        cur.execute('update listinginf set title=%s,deltatitle=%s where itemid=%s',(l['title'],'Yes',l['itemid']))
        conn.commit()
    if titleraw==l['title']:
        print '%s:the title is the same' % l['itemid']
        cur.execute('update listinginf set deltatitle=%s where itemid=%s',('No',l['itemid']))
        conn.commit()
    # cur.colse()
    conn.close()



class GetFiledsByItemid(GetFields):
    def _get_xml(self,item):
        fails=0

        while True:
            if fails>=15:
                break
            try:
                myrequest={'ItemID':item,'IncludeWatchCount':True}
                myresponse=self.mycon.execute('GetItem',myrequest)
                return myresponse.text
            except ConnectionError as e:
                print(e)
                print 'the GetItem Call of %s is die' % item
                fails+=1
            except (ssl.SSLError,requests.RequestException):
                print 'the request of %s via GetItem failed' % item
                fails+=1
            else:
                break
    def get_xml(self,item):
        i=0
        while i<10:
            try:
                myrequest={'ItemID':item,'IncludeWatchCount':True}
                myresponse=self.mycon.execute('GetItem',myrequest)
                return myresponse.text
            except:
                # print 'I will try again to request %s' % item
                i+=1
    def parse(self,itemid):
        patternCad=re.compile(r"<CategoryID>(.*?)</CategoryID>")
        patternCae=re.compile(r"<CategoryName>(.*?)</CategoryName>")
        patternCoy=re.compile(r'<Country>(.*?)</Country>')
        patternCuy=re.compile(r"<Currency>(.*?)</Currency>")
        patternCue=re.compile(r'>(\d+\.\d+)</CurrentPrice>')
        patternFee=re.compile(r'<FeedbackScore>(.*?)</FeedbackScore>')
        patternFer=re.compile(r"<FeedbackRatingStar>(.*?)</FeedbackRatingStar>")
        patternGal=re.compile(r"<GalleryURL>(.*?)</GalleryURL>")
        patternHit=re.compile(r"<HitCount>(.*?)</HitCount>")
        patternHir=re.compile(r'<HitCounter>(.*?)</HitCounter>')
        patternItd=re.compile(r"<ItemID>(.*?)</ItemID>")
        patternLon=re.compile(r"<Location>(.*?)</Location>")
        patternQud=re.compile(r"<QuantitySold>(.*?)</QuantitySold>")
        patternQue=re.compile(r"<QuantitySoldByPickupInStore>(.*?)</QuantitySoldByPickupInStore>")
        patternSht=re.compile(r'>(\d+\.\d+)</ShippingServiceCost>')
        patternShe=re.compile(r"<ShippingService>(.*?)</ShippingService>")
        patternSku=re.compile(r"<SKU>(.*?)</SKU>")
        patternSte=re.compile(r"<StartTime>(.*?)</StartTime>")
        patternStr=re.compile(r"<StoreOwner>(.*?)</StoreOwner>")
        patternStl=re.compile(r"<StoreURL>(.*?)</StoreURL>")
        patternTie=re.compile(r'<Title>(.*?)</Title>')
        patternUsd=re.compile(r'<UserID>(.*?)</UserID>')
        patternUse=re.compile(r'<Site>(.*?)</Site>')
        patternVil=re.compile(r'<ViewItemURL>(.*?)</ViewItemURL>')
        patternLin=re.compile(r'<ListingDuration>(.*?)</ListingDuration>')
        patternPrg=re.compile(r'<PrivateListing>(.*?)</PrivateListing>')
        xml=self.get_xml(itemid)
        expfileds=Fields()
        try:
            expfileds.fielddic['categoryid']=re.findall(patternCad,xml)[0]
            expfileds.fielddic['sku']=re.findall(patternSku,xml)[0][:11]
            expfileds.fielddic['categoryname']=re.findall(patternCae,xml)[0]
            expfileds.fielddic['country']=re.findall(patternCoy,xml)[0]
            expfileds.fielddic['currency']=re.findall(patternCuy,xml)[0]
            try:
                expfileds.fielddic['currentprice']=re.findall(patternCue,xml)[0]
            except IndexError:
                print xml
            expfileds.fielddic['feedbackscore']=re.findall(patternFee,xml)[0]
            expfileds.fielddic['feedbackstar']=re.findall(patternFer,xml)[0]
            expfileds.fielddic['galleryurl']=re.findall(patternGal,xml)[0]
            expfileds.fielddic['hitcount']=re.findall(patternHit,xml)[0]
            expfileds.fielddic['hitcounter']=re.findall(patternHir,xml)[0]
            expfileds.fielddic['itemid']=re.findall(patternItd,xml)[0]
            expfileds.fielddic['location']=re.findall(patternLon,xml)[0]
            expfileds.fielddic['quantitysold']=re.findall(patternQud,xml)[0]
            expfileds.fielddic['quantitysoldinstore']=re.findall(patternQue,xml)[0]
            try:
                expfileds.fielddic['shippingcost']=re.findall(patternSht,xml)[0]
            except IndexError:
                print xml
            expfileds.fielddic['shippingservice']=re.findall(patternShe,xml)[0]
            expfileds.fielddic['starttime']=re.findall(patternSte,xml)[0]
            expfileds.fielddic['storeowner']=re.findall(patternStr,xml)[0]
            expfileds.fielddic['storeurl']=re.findall(patternStl,xml)[0]
            expfileds.fielddic['title']=re.findall(patternTie,xml)[0]
            expfileds.fielddic['userid']=re.findall(patternUsd,xml)[0]
            expfileds.fielddic['usersite']=re.findall(patternUse,xml)[0]
            expfileds.fielddic['viewitemurl']=re.findall(patternVil,xml)[0]
            expfileds.fielddic['listduration']=re.findall(patternLin,xml)[0]
            expfileds.fielddic['privatelisting']=re.findall(patternPrg,xml)[0]
            return expfileds.fielddic
        except TypeError as e:
            print(e)


def threading4update():
    myqueue=fread()
    # starttime=time.time()
    # print starttime
    # for i in myqueue:
    #     print i
    #     print j
    #     j+=1
    try:
        pool=Pool(8)
        pool.map(update,myqueue)
    except AttributeError as e:
        print(e)
    except _mysql_exceptions.OperationalError as e:
        print(e)
    except IndexError as e:
        print(e)
    else:
        print 'there is something wrong. But I dont know what it is'
    pool.close()
    pool.join()
def update1(i):
    mygetfields=GetFiledsByItemid()
    l=mygetfields.parse(i[0])
    return  l
    # print '%s is put into the queue' % l
def concurrent():
    itemids=gen_list()
    with futures.ThreadPoolExecutor(max_workers=40) as executor:
        future_to_url={executor.submit(update1,item):item for item in itemids}
        for future in futures.as_completed(future_to_url):
            item=future_to_url[future]
            if future.exception() is not None:
                print "%s generated an exception:%s" % (item,future.exception)
            try:
                data=future.result()
                yield data
            except:
                print '%s has no future.result' % item
def single():
    for i in fread():
        update(i)

def fread():
     with open('/home/james/item','r') as myitem:
        for i in myitem.readlines():
            i=i.strip('\n')
            try:
                d=ast.literal_eval(i)
                yield d
            except:
                pass
def fwrite():
    with open('/home/james/item','a') as myitem:
        for l in concurrent():
            print l
            if l:
                myitem.write(str(l)+'\n')
def testupdate():
    for i in fread():
        # update(i)
        print i


def testcon():
    myconn=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebaydata')
    mycur=myconn.cursor()
    j=76247
    mycur.execute('select itemid from listinginf limit 76247,100000;')
    for i in mycur.fetchall():
        print i
        j+=1
    print j
    mycur.close()
    myconn.close()
def putinnow():


if __name__=='__main__':
    # fwrite()
    # for i  in fread():
    #     print i

    # testupdate()
    threading4update()
    # testcon()
    # single()



