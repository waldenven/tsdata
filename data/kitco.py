import urllib
from BeautifulSoup import BeautifulSoup
import re
from pandas import *
from datetime import datetime, date
import exceptions
from tsdata.data.basedata import basedata
from urllib import FancyURLopener

class MyOpener(FancyURLopener):
    version = 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.121 Safari/535.2'

def parsekitcourl(url, year):
    myopener = MyOpener()
    f = myopener.open(url)
    txt = f.read()
    f.close
    if year <= 2001:
        w = '90%'
    else:
        w = '75%'
    soup = BeautifulSoup(txt)
    ts = soup.findAll('table', width=w)
    if year >= 2006:
        datere = re.compile('([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])')
    else:
        datere = re.compile('([a-zA-Z]*) ([0-9]*), ([0-9][0-9][0-9][0-9])')
    trs=ts[0].findAll('tr')  
    df = DataFrame()
    for tr in trs:
        tds = tr.findAll('td')
        if len(tds) == 0:
            print 'skipping'
            continue
        m   = datere.match(tds[0].text)
        if m:
            if year >= 2006:
                d = date(year=int(m.group(1)),month=int(m.group(2)), day=int(m.group(3)))
            else:
                try:
                    d = datetime.strptime(m.group(0), '%B %d, %Y').date()
                except exceptions.ValueError:
                    g = m.group(0).replace('Sept', 'Sep')
                    d = datetime.strptime(g, '%b %d, %Y').date() # much of 2003
                        
            if len(tds) < 8:
                print 'skipping', ' '.join(map(lambda x: x.text, tds))
                continue
            s    = { 'gold_am'     : tds[1].text, 'gold_pm'      : tds[2].text,
                    'silver'       : tds[3].text,
                    'platinum_am'  : tds[4].text, 'platinum_pm ' : tds[5].text,
                    'palladium_am' : tds[6].text, 'palladium_pm' : tds[7].text }
            s2   = {}
            for k,v in s.iteritems():
                if type(v) == type('') or type(v) == type(u''):
                    try:           
                        s2[k] = float(v)
                    except exceptions.ValueError:
                        pass
                              
            rowdf = DataFrame(s2 , index=[d])
            df    = df.append(rowdf)
    return df    
    
class kitco(basedata):
    archivename = 'kitcoprec.pickle'
    tag         = 'kitco'
    earliest    = datetime(1996,1,1)
    chunktype   = 'YEAR'
    
    def _updateyear(self, y):
        today   = datetime.today()
        df      = DataFrame()
        if y != today.year:
            url = 'http://www.kitco.com/londonfix/gold.londonfix%02d.html' % (y%100)
        else:
            url = 'http://www.kitco.com/gold.londonfix.html'
        df      = parsekitcourl(url, y)
        df      = df.sort_index()
        return df 
