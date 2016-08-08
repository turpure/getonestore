__author__ = 'james'
import  datetime
'''define the fieds that we want to get'''
class Fields(object):
    fielddic={}
    fielddic['country']=''
    fielddic['currency']=''
    fielddic['hitcounter']=''
    fielddic['itemid']=''
    fielddic['starttime']=''
    fielddic['viewitemurl']=''
    fielddic['location']=''
    fielddic['categoryid']=''
    fielddic['categoryname']=''
    fielddic['feedbackscore']=''
    fielddic['feedbackstar']=''
    fielddic['usersite']=''
    fielddic['userid']=''
    fielddic['storeowner']=''
    fielddic['storeurl']=''
    fielddic['currentprice']=''
    fielddic['quantitysold']=''
    fielddic['quantitysoldinstore']=''
    fielddic['shippingservice']=''
    fielddic['shippingcost']='' #first priority
    fielddic['title']=''
    fielddic['hitcount']=''
    fielddic['sku']='' #it is different between mutiplvarations and single varation.
    fielddic['galleryurl']=''
    fielddic['listduration']=''
    fielddic['privatelisting']=''
    fielddic['curdate']=str(datetime.datetime.now())[:10]
    fielddic['deltatitle']='0'
    fielddic['deltasold']='0'
    fielddic['deltahit']='0'
    fielddic['deltaprice']='0'
    fielddic['listingstatus']=''



