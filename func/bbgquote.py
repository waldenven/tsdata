import urllib2, re, os
from pandas import DataFrame, Series, load
import datetime
from tsdata.util import applyrange
from tsdata.func.cache import wrap

def bbgquote_data(ticker):
    url = 'http://www.bloomberg.com/apps/data?pid=webpxta&Securities=%s&TimePeriod=5Y&Outfields=HDATE,PR005-H,PR006-H,PR007-H,PR008-H,PR013-H' % (ticker)
    f          = urllib2.build_opener(urllib2.HTTPCookieProcessor()).open(url)
    txt        = f.read()
    f.close()
    
    s = txt.splitlines()
    df = DataFrame()
    for row in s[1:-1]:
        d,c,o,h,l,v = row.split('"')
        d = int(d)
        date = datetime.date(year = d / 10000, month = (d / 100) % 100, day = d % 100)
        S = { 'date' : [date] }
        for key,val in zip(['close','open','high','low','volume'],[c,o,h,l,v]):
	    try:
		S[key] = [float(val.replace(',',''))]
	    except:
		pass
        df = df.append(DataFrame(S), ignore_index=True)
    return df

bbgquote_data_wrapped = wrap(bbgquote_data,str.upper)

def bbgquote(p, ticker, element='close'):
    df     = bbgquote_data_wrapped(ticker)
    ts  = Series(df[element], index=df['date'])
    return p.applyrange(ts)