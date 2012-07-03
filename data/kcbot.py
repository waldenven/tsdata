import urllib, re
from datetime import datetime
from pandas import DataFrame, Series, read_csv
from BeautifulSoup import BeautifulSoup
from tsdata.data.basedata import basedata

def all_daily_urls():
    url = 'http://www.kcbt.com/daily_wheat_price.asp'

    f = urllib.urlopen(url)
    txt = f.read()
    f.close()

    soup = BeautifulSoup(txt)
    ts = soup.findAll('table', { 'border' : 1, 'cellpadding' : "3", 'align' :"center", 'width' : "50%"})
    tds = soup.findAll('td', width='33%', nowrap='nowrap')
    return map(lambda x: x.a['href'], tds)

def one_date(url):
    f  = urllib.urlopen(url)
    df     = read_csv(f)
    df =df.rename(columns=lambda x: x.strip().lower()).dropna()
    df['date'] = map(lambda x: datetime.strptime(x.strip(), '%m/%d/%Y').date(), df['date'])
    for col in df.columns:
        if col not in ['exch', 'comid', 'date']:
            df[col] = map(lambda x: float(x), df[col])
        elif col in ['exch', 'comid']:
	    df[col] = map(lambda x: x.strip(), df[col])
    df['LYY'] = map(lambda m,y: 'FGHJKMNQUVXZ'[int(m-1)] + '%02d' % (y), df['month'], df['year'])
    return df

class kcbotfuts(basedata):
    archivename = 'kcbot.pickle'
    tag         = 'f'
    chunktype   = 'DAY'
    earliest    = datetime(2011,11,16)
    _cache      = None
    _changed    = False
    _updated    = False
    _scaling    = { k:0.01 for k in [ 'previous', 'open', 'high', 'low', 'close', 'settle' ] } # to match CBOT
    
    def handles(self, symbol):
	l = len(self.tag) + 1 + 2
	return symbol[:l].lower() == self.tag + '_' + 'kw'
    
    def parsesymbol(self, symbol):
        synre      = re.compile('%s_([^@_]*)_([^@]*)@(.*)' % self.tag )
        synrenoat  = re.compile('%s_([^@_]*)_([^@]*)'      % self.tag )
        
        m          = synre.match( symbol )
        if m:
            commod = m.group(1)
            month  = m.group(2)
            tag    = m.group(3)
        else:
            m          = synrenoat.match( symbol )
            commod = m.group(1)
            month  = m.group(2)
            tag    = 'settle'
        commod     = commod.upper()
        return { 'column' : tag, 'filter' : { 'comid' : commod, 'LYY' : month } }
    
    def _updateday(self, din):
	df  = DataFrame()
	url = 'http://www.kcbt.com/download/kcprccsv/kcprccsv_%4d%02d%02d.csv' % (din.year, din.month, din.day)
        return one_date(url)