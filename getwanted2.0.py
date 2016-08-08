__author__ = 'james'
__author__ = 'james'
from getitem import GetFields
from ebaydate import monthrange,timerange
from concurrent import  futures
from sellerid import erzou
from getlist import GetList
def gotwanted(m,n):
    myget=GetList()
    userids=erzou
    with futures.ThreadPoolExecutor(max_workers=20) as executor:
        for i in timerange(m,n):
            start=i[0]+'T07:52:46.000Z'
            end=i[1]+'T07:52:46.000Z'
            # for i in myfields.parse('betterlife-2010',start,end):
            future_to_url=dict((executor.submit(myget.get_list,userid,start,end),userid)for userid in userids)
            # print future_to_url
            # print future_to_url
            for future in futures.as_completed(future_to_url):
                userid=future_to_url[future]
                if future.exception() is not None:
                    print "%s generated an exception:%s" % (userid,future.exception)
                else:

                    yield future.result()
        # help(executor.submit)



if __name__=='__main__':
    print 'I am running'

    # for i in gotwanted(yafei,2):
    #     print i
    for i in gotwanted(1,10):
        print i






