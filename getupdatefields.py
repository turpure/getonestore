from getitem import GetFields
import re
from updatefields import Fields
import time
class GetUpdateFields(GetFields):
    def get_xml(self,items):
        if items:
            i=0
            while i<3:
                try:
                    myrequest={'ItemID':items,'IncludeWatchCount':True}
                    myresponse=self.mycon.execute('GetItem',myrequest)
                    if myresponse.text:
                        return myresponse.text
                    else:i+=1
                except:
                    # print '%s between %s and %s :fails %s' % (userid,starttimefrom,starttimeto,fails)
                    with open('/home/james/getlist.log','a') as log:
                        log.write("%s:%s fails %s \n" % (time.ctime(),items,i))
                    # print 'I will try again to request %s' % item
                    i+=1
    def parse(self,myxml):
        expfileds=Fields()
        patternCue=re.compile(r'>(\d+\.\d+)</CurrentPrice>')
        patternHit=re.compile(r"<HitCount>(.*?)</HitCount>")
        patternItd=re.compile(r"<ItemID>(.*?)</ItemID>")
        patternQud=re.compile(r"<QuantitySold>(.*?)</QuantitySold>")
        xml=myxml
        if xml:
            try:

                try:
                    expfileds.fielddic['currentprice']=re.findall(patternCue,xml)[0]
                except :
                    expfileds.fielddic['currentprice']=0

                expfileds.fielddic['hitcount']=re.findall(patternHit,xml)[0]
                expfileds.fielddic['itemid']=re.findall(patternItd,xml)[0]
                expfileds.fielddic['quantitysold']=re.findall(patternQud,xml)[0]

                return expfileds.fielddic
            except Exception as e:
                with open('/home/james/ebaydata.log','a') as log:
                    log.write('%s:%s\n' % (time.ctime(),e))
if __name__=='__main__':
    print GetUpdateFields().get_xml('311441771385')

