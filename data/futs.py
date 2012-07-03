import re, time, random, urllib, math, urllib2
from tsdata.data.basedata import basedata
from tsdata.data.kcbot import kcbotfuts
from pandas import DataFrame, Series
from BeautifulSoup import BeautifulStoneSoup, BeautifulSoup
from datetime import datetime
from urllib import FancyURLopener

LtoICEEUMON = { 'F' : 'Jan', 'G' : 'Feb', 'H' : 'Mar', 'J' : 'Apr', 'K' : 'May', 'M' : 'Jun',
           'N' : 'Jul', 'Q' : 'Aug', 'U' : 'Sep', 'V' : 'Oct', 'X' : 'Nov', 'Z' : 'Dec' }
LtoICEUSMON = {k:v.upper() for k,v in LtoICEEUMON.iteritems()}
LtoCMEMON   = LtoICEUSMON
LtoCMEMON['N'] = 'JLY'
           
mktbyexch = {
    'XCBT' : [ 'C', 'W', 'S', '06', '07', 'EH' ],
    'XCME' : [ 'LC', 'LN', 'FC', 'DA' ],
    'XNYM' : [ 'CL', 'NG', 'HO', 'RB', 'PL', 'PA' ],
    'XCEC' : [ 'GC', 'SI', 'HG' ]
    }
mktsubs = { 'SM' : '06', 'BO' : '07', 'AC' : 'EH', 'WTI' : 'CL', 'LH' : 'LN' }

def LYYtoCME(code):
    lyyre  = re.compile('([A-Z])([0-9][0-9])')
    m      = lyyre.match(code)
    return '%s %s' % (LtoCMEMON[m.group(1)], m.group(2))
   
def LYYtoICEUS(code):
    lyyre  = re.compile('([A-Z])([0-9][0-9])')
    m      = lyyre.match(code)
    return '%s%s' % (LtoICEUSMON[m.group(1)], m.group(2))

def LYYtoICEEU(code):
    lyyre  = re.compile('([A-Z])([0-9][0-9])')
    m      = lyyre.match(code)
    return '%s%s' % (LtoICEEUMON[m.group(1)], m.group(2))

def cmeurl(mkt, d):
    if mkt in mktsubs:
        mkt = mktsubs[mkt]
    exch = None
    for k,L in mktbyexch.iteritems():
	if mkt in L:
	    exch = k
	    break
    assert exch is not None
    t = datetime.now()
    url = 'http://www.cmegroup.com/CmeWS/mvc/xsltTransformer.do?'\
        + 'xlstDoc=/XSLT/da/DailySettlement.xsl&' \
        + 'url=/da/DailySettlement/V1/DSReport/ProductCode/%s/FOI/FUT/EXCHANGE/%s/Underlying/%s?' \
        + 'tradeDate=%02d/%02d/%4d&currentTime=%ld'
    return url % (mkt, exch, mkt, d.month, d.day, d.year,long(time.mktime(t.timetuple()))*1000 + int(random.random()*1000))
    
class MyOpener(FancyURLopener):
    version = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11)'
myopener = MyOpener()

def cmedata(mkt, d):
    url = cmeurl(mkt, d)
    f   = myopener.open(url)
    txt = f.read()
    f.close
    soup = BeautifulStoneSoup(txt)
    rows = soup.findAll('tr')

    df   = DataFrame()
    if len(rows) == 0:
        return df

    keys = map(lambda x: x.text, rows[1].findAll('th'))

    for row in rows[2:]:
        S     = {}
        items = row.findAll('td')
        items = map(lambda x: x.text, items)
        for item,k in zip(items,keys):
            if k == u'Month':
                S[k] = Series([item])
                continue
            if item == u'-' or item == u'':
                continue
            sp   = item.split("'")
		          #print sp
            if sp[0] == '-' or sp[0] == '+':
                v = 0
                if sp[0] == '-': sign = -1
                else: sign = 1
            elif sp[0] == u'UNCH':
                v = 0
            else:
                str  = sp[0].replace(',','').replace('+','')
                if str[-1] == 'A' or str[-1] == 'B': str = str[:-1]
                v    = float(str)
                sign = math.copysign(1,v)
            if len(sp) > 1:
                v    += float(sp[1][0]) / 8.0 * sign
            #print sp,v
            S[k] = Series([v])
        df = df.append(DataFrame(S),ignore_index=True)
        df['date']   = Series(d,index=df.index)
        df['symbol'] = Series(mkt,index=df.index)
    return df
    
class cmefuts(basedata):
    archivename = 'cmeinitial.pickle'
    tag         = 'f'
    chunktype   = 'DAY'
    earliest    = datetime(2011,11,1)
    _cache      = None
    _changed    = False
    _updated    = False
    
    markets = [ 'C', 'W', 'S', '06', '07', 'CL', 'NG', 'RB', 'HO', 'LC', 'LN', 'FC', 'DA', 'EH' ]
    
    def LYYtoExch(self, month):
	return LYYtoCME(month)
    
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
            tag    = 'Settle'
        commod = mktsubs.get(commod, commod)
        return { 'column' : tag, 'filter' : { 'symbol' : commod, 'Month' : self.LYYtoExch(month) } }
    
    def handles( self, symbol ):
	if symbol in self.markets:
	    return True
        ps = self.parsesymbol(symbol)
        return ps['filter']['symbol'] in self.markets
        
    def _updateday(self, d):
	df  = DataFrame()
	for L in mktbyexch.itervalues():
	    for mkt in L:
		seconds = random.uniform(15, 45)
		print 'Sleeping for %g seconds before fetching %s' % (seconds, mkt)
		time.sleep(seconds)
		newdf = cmedata(mkt, d)
		df    = df.append(newdf, ignore_index=True)
	return df

class iceusfuts(basedata):
    archivename = 'iceusinitial.pickle'
    tag         = 'f'
    chunktype   = 'DAY'
    earliest    = datetime(2011,11,8)
    _cache      = None
    _changed    = False
    _updated    = False
    
    markets = [
       'AR', 'AS', 'CC', 'CI', 'CR', 'CT', 'DX', 'ER', 'EZ', 'GN', 'HR', 'HY', 'IAU',
       'IEJ', 'IEO', 'IEP', 'IGB', 'IKX', 'IMF', 'IMP', 'IRK','IRZ', 'ISN', 'ISV',
       'KAU', 'KC', 'KCU', 'KEJ', 'KEO', 'KEP', 'KGB', 'KJ', 'KMF', 'KMP', 'KOL','KRA',
       'KRK', 'KRZ', 'KSN', 'KSV', 'KX', 'KY', 'KZX', 'KZY', 'MF', 'MP', 'NJ','NR',
       'NT', 'OJ', 'PC', 'PK', 'PS', 'PZ', 'QA', 'RF', 'RG', 'RV', 'SB', 'SF', 'SN', 'SS',
       'SV', 'SY', 'TF', 'TR', 'VC', 'VU', 'YA', 'YZ', 'ZJ', 'ZR','ZX', ]
    
    def LYYtoExch(self, month):
	return LYYtoICEUS(month)
    
    def handles( self, symbol ):
	if symbol in self.markets:
	    return True
        ps = self.parsesymbol(symbol)
        return ps['filter']['symbol'] in self.markets
    
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
            tag    = 'Settle'
        return { 'column' : tag, 'filter' : { 'symbol' : commod, 'Month' : self.LYYtoExch(month) } }
    
    def isfinal(self, d):
	# Need to have OI data
	df = self.data()
	df = df[df['date'] == d]
	df = df[df['Month'] != 'total']
	print len(df), df['OI'].count(), df['Settle'].count()
	return df['OI'].count() == df['Settle'].count()
        
    def _updateday(self, d):
	df  = DataFrame()
	url = 'https://www.theice.com/marketdata/nybotreports/getFuturesDMRResults.do'
	query_args = {
	    'tradeYear':       '%d' % (d.year), 
	    'tradeMonth':      '%d' % (d.month-1),
	    'venueChoice':     'Electronic',
	    'tradeDay':        '%d' % (d.day), 
	    'commodityChoice': 'ALL COMMODITIES'}
	encoded_args = urllib.urlencode(query_args)
	txt          = urllib2.urlopen(url, encoded_args).read()
	
	soup    = BeautifulSoup(txt)
	ts      = soup.findAll('table')
	rows    = ts[1].findAll('tr')
	df      = DataFrame()
	totalre = re.compile('Totals for ([^ ]*)[ :].*')
	allkeys = ['symbol', 'Month', 'Open', 'High', 'Low', 'Close', 'Settle', 'Change', 'Volume', 'OI',
		    'OI Change', 'ADJ', 'EFP', 'EFS', 'Block Trades', 'Spread Volume', 'Contract High', 'Contract Low']
	totkeys = ['symbol', 'Volume', 'OI', 'OI Change', 'DISCARD', 'EFP', 'EFS', 'Block Trades',
	            'Spread Volume' ]
	for row in rows:
	    ths = row.findAll('th')
	    if len(ths) > 0:
		continue
	    tds = row.findAll('td')
	    if len(tds) == 1:
		continue
	    S = {}
	    items = map(lambda x: x.text, tds)
	    m = totalre.match(items[0])
	    if m:
		keys = totkeys
		S['symbol' ] = Series([m.group(1)])
		S['Month' ]  = Series(['total'])
	    else:
		keys = allkeys
	    for k, item in zip(keys, items):
		if item == '&nbsp;' or item == 'N/A':
		    continue
		if k == 'symbol' and m:
		    continue
		if k == 'Month' or k == 'symbol':
		    S[k] = Series([item])
		else:
		    v = float(item.replace(',','').replace('*',''))
		    S[k] = Series([v])
	    df = df.append(DataFrame(S),ignore_index=True)
	df['date']=Series(d,index=df.index)
	return df
	
class iceeufuts(basedata):
    archivename = 'iceeuinitial.pickle'
    tag         = 'f'
    chunktype   = 'DAY'
    earliest    = datetime(2011,10,17).date()
    _cache      = None
    _changed    = False
    _updated    = False
    
    markets     = [ 'BRT', 'GO' ]
    
    def LYYtoExch(self, month):
	return LYYtoICEEU(month)
    
    def handles( self, symbol ):
	if symbol in self.markets:
	    return True
        ps = self.parsesymbol(symbol)
        return ps['filter']['commod'] in self.markets
    
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
            tag    = 'Sett'
        return { 'column' : tag, 'filter' : { 'commod' : commod, 'Month' : self.LYYtoExch(month) } }
    
    def _updateday(self, d):
	df  = DataFrame()
	
	url = 'https://www.theice.com/marketdata/reports/icefutureseurope/EndOfDay.shtml'
	query_args = {
	    'tradeYear':       '%d' % (d.year), 
	    'tradeMonth':      '%d' % (d.month-1),
	    'tradeDay':        '%d' % (d.day), 
	    'index' :          'Submit',
	    }
	keys = { 'BRT' : 1, 'GO' : 3}
	for commod, k in keys.iteritems():
	    query_args['contractKey'] = k
	    encoded_args = urllib.urlencode(query_args)
	    txt          = urllib2.urlopen(url, encoded_args).read()
	    soup         = BeautifulSoup(txt)
	    
	    gt           = soup.findAll('span', {'class' : 'GeneralText'})
	    datere       = re.compile('.*([0-9][0-9]-[A-Z][a-z]*-[0-9][0-9][0-9][0-9])')
	    m            = datere.match(gt[0].text)
	    resultd      = datetime.strptime(m.group(1), '%d-%b-%Y').date()
	    if resultd != d:
		continue
	    
	    ts           = soup.findAll('table')
	    t            = ts[1]
	    rows         = t.findAll('tr')
	    keys         = map(lambda x: x.text, rows[0]('th'))
	    data0        = map(lambda x: x.text, rows[1]('td'))
	    #headers, data0
            #print keys
	    for row in rows[1:]:
		S = { 'commod' : Series([commod]), 'date' : Series([d]) }
		items = row.findAll('td')
		items = map(lambda x: x.text, items)
                #print '|'.join(items)
		for item,k in zip( items, keys ):
		    # need remove date from prev day vol....
		    if len(item) == 0 or item == '&nbsp;':
			continue
                    if 'Expired Contract' in item:
                        print 'Skipping expired contract %s' % (S)
                        continue
		    if k == u'Month':
			S[k]  = Series([item])
		    elif 'Prev Day' in k:
			S['Prev Day Vol'] = Series([float(item.replace(',',''))])
		    else:
			#print '#%s#' % (item)
			S[k]  = Series([float(item.replace(',',''))])
		df = df.append(DataFrame(S),ignore_index=True)
	return df
    
class futs:
    helpers = [ kcbotfuts(), cmefuts(), iceusfuts(), iceeufuts() ]
    
    def helper(self, symbol):
	for h in self.helpers:
	    if h.handles(symbol):
		return h
        raise NameError(symbol)
        
    def ts(self, symbol):
	h = self.helper(symbol)
	return h.ts(symbol)
		
