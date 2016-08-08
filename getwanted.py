__author__ = 'james'
from getitem import GetFields
from ebaydate import timerange
from multiprocessing.dummy import Pool
from sellerid import yafei
myfields=GetFields()
def gotwanted(userid):

    for i in timerange(10):
        item=myfields.parse(userid,i[0]+'T07:52:46.000Z',i[1]+"T07:52:46.000Z")
        for l in item:
            yield l


if __name__=='__main__':
    print 'I am running'
    pool=Pool(4)
    pool.map(gotwanted,yafei)
    pool.close()
    pool.join()



