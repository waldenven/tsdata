import re
from tsdata.data.basedata import basedata
from tsdata.data.djubs import djubs
from datetime import datetime
import urllib, urllib2
from BeautifulSoup import BeautifulSoup
from pandas import DataFrame, Series

class spgsci(basedata):
    archivename = 'spgsci.pickle'
    tag         = 'ix'
    chunktype   = 'DAY'
    earliest    = datetime(2011,11,16)
    _cache      = None
    _changed    = False
    _updated    = False
    
    indexnamemap = {
    'GSCI' :                           'spgsci',
    'GSCIAgriculture&Livestock' :      'spgsal',
    'GSCIE139' :                       'spgse139',
    'Livestock' :                      'spgslv',
    'GSCICrude' :                      'spgscl',
    'PreciousMetals' :                 'spgspm',
    'Agriculture' :                    'spgsag',
    'Energy' :                         'spgsen',
    'Agriculture&Livestock' :          'spgsal', # Dupe
    'GSCIUltraLightEnergyStrategy27' : 'spgsulene27',
    'GSCIUltra-LightEnergy' :          'spgsule',
    'GSCIEnhancedCommodityIndex' :     'spgenhi',
    'GSCILightEnergy' :                'spgsle',
    'GSCIJPY' :                        'spgscijy',
    'GSCIReducedEnergy' :              'spgsre',
    'GSCIAgricultureJPY' :             'spgsagjy',
    'GSCINon-Energy' :                 'spgsne',
    'Non-Energy' :                     'spgsne', # Dupe
    'IndustrialMetals' :               'spgsim',
    'GSCIE10' :                        'spgse10',
    }
    suffixmap = { 'Spot' : '', 'Excess Return' : 'p', 'Total Return' : 'tr' }
    
    def handles(self, symbol):
	l = len(self.tag) + 1 + 4
	return symbol[:l] == self.tag + '_' + 'spgs'

    def parsesymbol(self, symbol):
        synrenoat  = re.compile('%s_([^@_]*)'      % self.tag )
        
	m      = synrenoat.match( symbol )
	index  = m.group(1)
	tag    = 'Settle'
        return { 'column' : tag, 'filter' : { 'index' : index } }
        
    def _updateday(self, din):
        url = 'http://www2.goldmansachs.com/gsci/insert.en.html'
	f = urllib2.build_opener(urllib2.HTTPCookieProcessor()).open(url)
	bin = f.read()
	f.close()
	soup = BeautifulSoup(bin)
	ts = soup.findAll('table')
	numberre   = re.compile('Table [1-9]:.*\((.*)\)')
	S = {}
	df = DataFrame()
	for t in ts:
	    td = t.find('td')
	    m  = numberre.match(td.text)
	    if m:
		d  = datetime.strptime(m.group(1), '%B %d, %Y').date()
		print d
		if d != din:
		    print 'Skipping as date %s is not as requested %s' % (d, din)
		    continue
		print td.text
		nt = t.findNextSibling('table')
		subts = nt.findAll('table')
		for subt in subts[1:]:
		    rows = subt.findChildren('tr')
		    L = {}
		    if rows[1].text != '&nbsp;':
			L['Spot']      = float(rows[1].text)
		    L['Excess Return'] = float(rows[2].text)
		    L['Total Return']  = float(rows[3].text)
		    name = rows[0].text
		    name = name.replace('&trade;','')
		    name = name.replace('&amp;','&')
		    index = self.indexnamemap[name.replace(' ','').replace('S&P','')]
		    S[name]    = L
		    for k,v in L.iteritems():
			df = df.append(DataFrame({ 'type' : Series([k]), 'Settle' : Series([v]),
						'index name' : Series([name]),
						'index' : Series([index + self.suffixmap[k]]),
						'date' : Series([d])}), ignore_index=True)
	return df

class commodindex:
    helpers = [ spgsci(), djubs() ]
    
    def helper(self, symbol):
	for h in self.helpers:
	    if h.handles(symbol):
		return h
	raise NameError(symbol)
    
    def ts(self, symbol):
        h = self.helper(symbol)
        return h.ts(symbol)
