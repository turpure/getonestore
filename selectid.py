__author__ = 'james'
import MySQLdb

def connection(query):
    con=MySQLdb.connect(host='192.168.1.129',user='root',passwd='urnothing',db='ebaydata')
    cur=con.cursor()
    cur.execute(query)
    outcome=cur.fetchall()
    return outcome
if __name__=='__main__':
    query='select tem.userid from (select userid,count(*) as row from listinginf group by userid having\
        row<1000) as tem'
    with open('recallid.py','a') as userlist:
        userlist.write('selectedid=[\n')
        for id in list(connection(query)):
            userlist.write("'%s',\n" % id)
        userlist.write(']\n')



