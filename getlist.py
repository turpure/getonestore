__author__ = 'james'
from ebaysdk.trading import Connection
import re
import time
class GetList(object):
    # faieldlisting=Queue.Queue()
    def __init__(self,
                 domain='api.ebay.com',
                 appid='ZhouPeng-3242-4cc7-88fd-310f513fcd71',
                 devid='df3f2898-65b1-4e15-afd5-172b989903aa',
                 certid='a0e19cf9-9b2b-457f-b6f1-87f3f600ca63',
                 token='AgAAAA**AQAAAA**aAAAAA**cJyLVQ**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6wMkoqiC5mHoQmdj6x9nY+seQ**N+QCAA**AAMAAA**nLCwNt4AQt1TRtd2ydNIMuZ2JYnQZKVVYarn41QQfBSqccEDld22ltKr+C/HJTN8AKD4+jn/nIEqtjNMkmh9sxTIa6jVVLAH5sN/93X7gTCmTkOsE/Av612U90nRoyQJ5bX1+NO25tMDZs9U0aTJIwVVu1BAB8/nsjL0pTCWw7KZACJ+a/aQ6swXLvSvOCWIBjFyCaWibKZseT2LoJMvJQmC2QpIuDsQ8cTYozLUYZqC88uKAjo7DNWIvaPVCdwkp/Vux3arR1Asin4ewX1l+LWCamWsXeBiVyaYq/oEUXABgknieVAPEpaFAfSzlrcTNmTWLBDDRwRGI/8hJCwK6/eJWexGrLk7U7p0kRltktNseTckAKT7g1ED4C5gUeQ4/nTHsNQBejUPTzTlwBWTJpwRaBFD7dAlbagH+TKaEJK41Esf/ZpL2599LUMolsO8tBgo0BhtCF/bYtdUUfopksIKNUwPXikadUxx6TurknnTtR1WDD229uUJIIf9BCS68WB56OfDTdXcZ8rdPZ0zdHuw5+BRxrumpFUzTQb5fJeHRDLPtQbLdX5rFPrS+NPJl6Qzi7bWNxUCydNQcIzKv7xLquIPoPx8bD1PRCoQbzjTQsOqhe9PBvLtcJ0Ggve78YjQKb5nDt6YThZ6D+1EOKdcthU03VizDfBKBLJ/NPqktDTx74tsS3l4feAjblGDoQ2RZXefJ9Jk3t+Qc4khlvl4mKpjZ4sCakh4qPWr9H6t3CN80hz5MO1Y7uHPUY61',
                 # timeout=3
                 ):
        self.idomain=domain
        self.iappid=appid
        self.idevid=devid
        self.icertid=certid
        self.itoken=token
        # self.timeout=
        self.mycon=Connection(domain=self.idomain,
                            appid=self.iappid,
                            devid=self.idevid,
                            certid=self.icertid,
                            token=self.itoken,
                            timeout=3
                            )

    def get_list(self,userid,starttimefrom,starttimeto):

        request={'UserID':userid,"StartTimeFrom":starttimefrom,'StartTimeTo':starttimeto}
        pattern=re.compile(r'<ItemID>(.*?)</ItemID>')
        fails=0
        # tstart=time.time
        while fails<8:

            try:
                myresponse=self.mycon.execute('GetSellerList',request)
                itemarry=re.findall(pattern,myresponse.text)
                if itemarry:
                    return itemarry
                else:
                    print '%s between %s and %s :empty itemarry' (userid,starttimefrom,starttimeto)
                    fails+=1
            # except Exception as e:
            except:
                # print(e)
                print '%s between %s and %s :fails %s' % (userid,starttimefrom,starttimeto,fails)
                with open('/home/james/getlist.log','a') as log:
                    log.write(time.ctime()+':'+userid+'between'+starttimefrom+'and'+starttimeto+':fails'+str(fails)+'\n')
                fails+=1


if __name__=="__main__":
    from ebaydate import monthrange
    mylist=GetList()
    for i in monthrange(1,2):
        a= mylist.get_list('naturalgemstone2009',i[0]+'T15:02:52.000Z',i[1]+'T15:02:52.000Z')

        if a:
            print a
        # print len(a)