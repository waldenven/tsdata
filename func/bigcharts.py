import urllib, urllib2, re
from datetime import datetime
from pandas import DataFrame, Series
from xlrd import xldate_as_tuple
from datetime import datetime
from tsdata.util import dropdupe
from tsdata.func.cache import wrap

def bigcharts_data(country, ticker):
    t           = datetime.now()
    begin       = t.replace(year=t.year-8)
    postArgs    = { 'endDate'   : t.strftime('%m/%d/%y 18:59:59'),
                 'beginDate' : begin.strftime('%m/%d/%y 19:00:00'),
                 #'beginDate' : datetime(1998,1,1,14,0),
                 'ticker'    : ticker,
                 'countryCode' : country,
                 'frequency' : 5,
                 'type'      : 1,
                }
    if re.match('BRT[0-9][A-Z]',ticker):
        postArgs.beginDate = begin.replace(year=t.year-5).strftime('%m/%d/%y 19:00:00') 
    encoded_args =  urllib.urlencode(postArgs).replace('+','%20')
    encoded_args += '&docSetUri=90&docSetUri=103&docSetUri=159&docSetUri=173&docSetUri=183&docSetUri=184&docSetUri=3126&docSetUri=436&docSetUri=2988'
    
    headers      = { 'User-Agent' : 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11',
           'Referer'     : 'http://bc.wsj.net/content/v0007/swf/interactivechart.swf',
           'Origin'      : 'http://bc.wsj.net',
           'Host'        : 'www.marketwatch.com',
           }
           
    url          = 'http://www.marketwatch.com/thunderball.flashcharter/JsonHandler.ashx'
    req          = urllib2.Request(url, encoded_args, headers)

    f            = urllib2.urlopen(req)
    txt          = f.read()
    
    datare       = re.compile('"TimeSeriesOhlcDataPoint":\\[([^]]*)\\]')
    m            = datare.search(txt)
    data         = m.group(1)
    itemre       = re.compile('{([^}]*)}')
    m2           = itemre.findall(data)

    numsre       = re.compile('[1-9][0-9]*')
    df           = DataFrame()
    for row in m2[1:]:
	pair_strs = row.split(',')
	pairs = []
	for ps in pair_strs:
	    ix = ps.index(':')
	    a,b = ps[1:ix-1], ps[ix+1:]
	    pairs.append( (a, b) )
	S = {}
	for p in pairs:
	    if p[1] == '' or 'UTime' in p[0]:
		continue
	    elif 'Date' not in p[0]:
		v = float(p[1])
	    else:
		m = numsre.search(p[1])
		v = float(m.group(0))
		v = datetime.fromtimestamp(v/1000)
	    S[p[0]] = Series([v])
	df = df.append(DataFrame(S),ignore_index=True)
    ds         = df['EndDate']
    df['date'] = map(lambda x: x.date(), ds)
    if df[-1:]['date'] == df[-2:-1]['date']:
	df     = df[:-1]
    return df
 
bigcharts_data_wrapped = wrap(bigcharts_data)
    
def bigcharts(p, country, ticker, element='Last'):
    df         = bigcharts_data_wrapped( country, ticker )
    return p.applyrange(Series(df[element],index=df['date'],name='%s:%s' %(country,ticker)))
