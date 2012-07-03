from pandas import DataFrame, Series
from datetime import datetime
import urllib, urllib2, re
from tsdata.data.basedata import basedata

def djubs_oneid(id):
    query_args = {'method': 'getIndexes', 'familyId': str(id) }
    encoded_args = urllib.urlencode(query_args)
    
    headers = { 'User-Agent' : 'Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11',
	    'Origin'  : 'http://www.djindexes.com',
	    'Host'    : 'www.djindexes.com',
	    'Referer' : 'http://www.djindexes.com/commodity/' }

    url = 'http://www.djindexes.com/DataService/v2/Intraday.cfc'

    req = urllib2.Request(url, encoded_args, headers)

    f   = urllib2.urlopen(req)
    txt = f.read()

    listre = re.compile('\[[^]]*\]')
    m   = listre.search(txt)
    itemre = re.compile('{([^}]*)}')
    m2  = itemre.findall(m.group(0))

    df = DataFrame()
    for row in m2:
	pair_strs = row.split(',')
	pairs = []
	for ps in pair_strs:
	    ix = ps.index(':')
	    a,b = ps[1:ix-1], ps[ix+2:-1]
	    pairs.append( (a, b) )
	S = {}
	for p in pairs:
	    if p[0] in [ 'last', 'netchange', 'percentagechange' ]:
		v   = float(p[1])
	    elif p[0] in [ 'o', 'bigchartsid', 'hascomponents', 'hasproducts', 'realtime' ]:
		v   = int(p[1])
	    elif p[0] == 'lasttime':
		v = datetime.strptime(p[1].strip(), '%d %b %H:%M')
		v = v.replace( year=datetime.today().year )
	    else:
		v =  p[1]
	    S[p[0]] = Series([v])
	df = df.append(DataFrame(S),ignore_index=True)
    df['date'] = Series(map(lambda x: x.date(),df['lasttime']))
    return df

class djubs(basedata):
    archivename = 'djubs.pickle'
    tag         = 'ix'
    chunktype   = 'DAY'
    earliest    = datetime(2011,11,16)
    _cache      = None
    _changed    = False
    _updated    = False
    
    def handles(self, symbol):
	l = len(self.tag) + 1 + 2
	return symbol[:l] == self.tag + '_' + 'dj'
    
    def parsesymbol(self, symbol):
        synrenoat  = re.compile('%s_([^@_]*)'      % self.tag )
        
        m      = synrenoat.match( symbol )
        index  = m.group(1)
        tag    = 'last'
        return { 'column' : tag, 'filter' : { 'ticker' : index.upper() } }
    
    def _updateday(self, din):
	df = DataFrame()
	for id in [8,9,10]:
	    newdf = djubs_oneid(id)
	    df    = df.append(newdf,ignore_index=True)
	return df[df['date']==din]
