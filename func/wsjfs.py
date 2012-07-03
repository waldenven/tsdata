import urllib, re
from BeautifulSoup import BeautifulSoup
from pandas import DataFrame, Series
from datetime import datetime
from tsdata.func.cache import wrap

# Data from main page: http://online.wsj.com/mdc/public/page/2_3028.html?category=Energy&subcategory=Petroleum&mod=topnav_2_3023
#Examples
#CL H2 = WTI H12

def wsjfs_data(symbol):
    coded = urllib.quote(symbol)

    url = 'http://ifs.futuresource.com/charts/charts.jsp?cID=WSJ&iFSsymbols=%s&iFScompareTo=&iFSperiod=D&iFSvminutes=&iFSchartsize=800x550&iFSbardensity=LOW&iFSbartype=BAR&iFSstudies=&iFSohlc=true' % (coded)
    print url

    f    = urllib.urlopen(url)
    txt  = f.read()
    soup = BeautifulSoup(txt)
    ars  = soup.findAll('area')
    data = map( lambda x: x['onmouseover'], ars)
    splitre = re.compile('Date: *([0-9/]*) *Open: *([0-9.]*) *High: *([0-9.]*) *Low: *([0-9.]*) *Close: *([0-9.]*)')
    df = DataFrame()
    for row in data:
	m = splitre.search(row)
	if not m:
	    continue
	S = { 'date' : Series([datetime.strptime(m.group(1), '%m/%d/%Y').date()]) }
	for k,v in zip( ['open', 'high', 'low', 'close' ], [ m.group(2), m.group(3), m.group(4), m.group(5) ] ):
	    S[k] = Series([float(v)])
	df = df.append(DataFrame(S), ignore_index=True)
    return df

wsjfs_data_wrapped = wrap(wsjfs_data)
    
def wsjfs(p, ticker, element='close'):
    df         = wsjfs_data_wrapped( ticker )
    return p.applyrange(Series(df[element],index=df['date'],name='%s'%(ticker)))