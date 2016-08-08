from getitem import GetFields
import re
from fields import Fields
import time
class GetFiledsByItemid(GetFields):
    def get_xml(self,items):
        if items:
            i=0
            while i<4:
                try:
                    myrequest={'ItemID':items,'IncludeWatchCount':True}
                    myresponse=self.mycon.execute('GetItem',myrequest)
                    return myresponse.text
                except:
                    print '%s between %s and %s :fails %s' % (userid,starttimefrom,starttimeto,fails)
        
                    # print 'I will try again to request %s' % item
                    i+=1
    def parse(self,myxml):
        expfileds=Fields()
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
        patternLis=re.compile(r'<ListingStatus>(.*?)</ListingStatus>')
        xml=myxml
        if xml:
            try:
                try:
                    expfileds.fielddic['categoryid']=re.findall(patternCad,xml)[0]
                except Exception as e:
                    print e
                    expfileds.fielddic['categoryid']="000000"
                try:
                    expfileds.fielddic['sku']=re.findall(patternSku,xml)[0][:11]
                except Exception as e:
                    print e
                    expfileds.fielddic['sku']='Nosku'
                try:
                    expfileds.fielddic['categoryname']=re.findall(patternCae,xml)[0]
                except Exception as e:
                    print e
                    expfileds.fielddic['categoryname']="ebay"
                try:
                    expfileds.fielddic['country']=re.findall(patternCoy,xml)[0]
                except Exception as e:
                    print e
                    expfileds.fielddic['country']="Universe"
                try:
                    expfileds.fielddic['currency']=re.findall(patternCuy,xml)[0]
                except Exception as e:
                    print e 
                    expfileds.fielddic['currency']="unkown"
                try:
                    expfileds.fielddic['currentprice']=re.findall(patternCue,xml)[0]

                except Exception as e:
                    print e
                    expfileds.fielddic['currentprice']='0'
                try:
                    expfileds.fielddic['feedbackscore']=re.findall(patternFee,xml)[0]
                except Exception as e:
                    print e
                    expfileds.fielddic['feedbackscore']='0'
                try:
                    expfileds.fielddic['feedbackstar']=re.findall(patternFer,xml)[0]
                except Exception as e:
                    print e
                    expfileds.fielddic['feedbackstar']="unknow"
                try:
                    expfileds.fielddic['galleryurl']=re.findall(patternGal,xml)[0]
                except Exception as e:
                    print e
                    expfileds.fielddic['galleryurl']="unknow"
                try:
                    expfileds.fielddic['starttime']=re.findall(patternSte,xml)[0]
                    expfileds.fielddic['hitcount']=re.findall(patternHit,xml)[0]
                    expfileds.fielddic['hitcounter']=re.findall(patternHir,xml)[0]
                    expfileds.fielddic['itemid']=re.findall(patternItd,xml)[0]
                    expfileds.fielddic['location']=re.findall(patternLon,xml)[0]
                    expfileds.fielddic['quantitysold']=re.findall(patternQud,xml)[0]
                    expfileds.fielddic['quantitysoldinstore']=re.findall(patternQue,xml)[0]
                except Exception as e:
                    print e
                try:
                    expfileds.fielddic['shippingcost']=re.findall(patternSht,xml)[0]
                except IndexError:
                    # print xml
                    expfileds.fielddic['shippingcost']="0"
                expfileds.fielddic['shippingservice']=re.findall(patternShe,xml)[0]
                expfileds.fielddic['starttime']=re.findall(patternSte,xml)[0]
                expfileds.fielddic['storeowner']=re.findall(patternStr,xml)[0]
                try:
                    expfileds.fielddic['storeurl']=re.findall(patternStl,xml)[0]
                except:
                    expfileds.fielddic['storeurl']='Nostoreurl'
                expfileds.fielddic['title']=re.findall(patternTie,xml)[0]
                expfileds.fielddic['userid']=re.findall(patternUsd,xml)[0]
                expfileds.fielddic['usersite']=re.findall(patternUse,xml)[0]
                expfileds.fielddic['viewitemurl']=re.findall(patternVil,xml)[0]
                expfileds.fielddic['listduration']=re.findall(patternLin,xml)[0]
                expfileds.fielddic['privatelisting']=re.findall(patternPrg,xml)[0]
                expfileds.fielddic['listingstatus']=re.findall(patternLis,xml)[0]
                return expfileds.fielddic
            except Exception as e:
                print "can't parse xml"


