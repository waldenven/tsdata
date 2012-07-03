import time, datetime, urllib2
from table_parser import *
from BeautifulSoup import BeautifulSoup
import unicodedata
import numpy as np
from pandas import *

def ponderurl( d ):
    s = 'http://domino.research.ibm.com/Comm/wwwr_ponder.nsf/Challenges/%s%s.html'
    return s % ( d.strftime('%B'), d.strftime('%Y') )

def namequirks( n ):
    n = n.replace('&#263;','c')
    n = n.replace('&amp;','&')
    n = n.replace('&#287;','g')
    n = n.replace('&scaron;','s')
    n = n.replace('&#321;','L')
    n = n.replace('&#324;','n')
    n = n.replace('&#328;','n')
    n = n.replace('&rsquo;',"'")
    quirks = {
              'Kipp Johnson& Nick Vigo'   : 'Kipp Johnson & Nick Vigo',
              'Nick Vigo and Kipp Johnson': 'Nick Vigo & Kipp Johnson',
              'Prathu Bharti Tiwari'      : 'Prithu Bharti Tiwari',
              'Prithu Tiwari'             : 'Prithu Bharti Tiwari',
              'Siddharth S'               : 'Siddharth S.',
              'Gale W. Greenlee'          : 'Gale Greenlee',
              'John G Fletcher'           : 'John G. Fletcher',
              'Victor A Chang'            : 'Victor A. Chang',
              'Victor Chang'              : 'Victor A. Chang',
              'Daniel Chong Jyh'          : 'Daniel Chong Jyh Tar',
              'Adam Mosquera'             : 'Adam M. Mosquera',
              'Donald T Dodson'           : 'Donald T. Dodson',
              'Eric Farmer'               : 'Eric R. Farmer',
              'Blatter  Christian'        : 'Christian Blatter',
              }
    if n in quirks:
        n = quirks[ n ]
    return n

# Note we are ignoring TZ here
def parsetime( str ):
    str   = str.replace( '@','' )
    str   = str.replace( '04/302009', '04/30/2009')
    str   = str.replace( '10/11/2009 02:11', '10/11/2009 02:11 AM' )
    str   = str.replace( '05/2009 12:24 PM', '05/03/2009 12:24 PM' )
    str   = str.replace( '05/042009', '05/04/2009' )
    str   = str.replace( '11.14.2006 02:16:52', '11/14/2006 02:16:52 PM' )
    str   = str.replace( '02.08.2007 07.15.56 PM', '02.08.2007 07:15:56 PM' )
    str   = str.replace( '03.24.2007 o5:25:25 PM', '03.24.2007 05:25:25 PM' )
    str   = str.replace( '04:23:2007 09:47:35 AM', '04/23/2007 09:47:35 AM' )
    str   = str.replace( '08.05.2007 02:09:23:22 AM', '08/05/2007 02:09:23 AM' )
    str   = str.replace( '08.15.2007 002:33:37 PM', '08/15/2007 02:33:37 PM' )
    str   = str.replace( '(','' )
    str   = str.replace( 'Partial solution) ', '' )
    
    formats = ( '%m/%d/%Y %I:%M %p', '%m/%d/%Y %I%M %p', '%m/%d/%Y %H:%M %p', '%m/%d/%Y %H%M %p',
               '%m/%d/%Y %I:%M:%S %p', '%m.%d.%Y %I:%M:%S %p', '%m.%d.%Y %I:%M %p',
               '%m.%d.%Y %I:%M:%S%p', '%m.%d.%Y %H:%M:%S %p' )
    for format in formats:
        try:
            t     = time.strptime(str, format)
        except ValueError:
            continue
        return( t )
    raise Exception( 'bad time str |' + str + '|' )

def pondersolvers( d ):
    if d.year == 2005 and d.month in (4,7,8):  # none these months
        return DataFrame()
    url = ponderurl( d )
    #print url
    f = urllib2.build_opener(urllib2.HTTPCookieProcessor()).open(url)
    txt = f.read()
    p = TableParser()
    p.feed(txt)
    f.close()
    
    soup = BeautifulSoup(txt)
    pplwho   = soup.find(text=re.compile('^People who'))
    if pplwho == None:
        pplwho   = soup.find(text=re.compile('^We are not'))
    parent   = pplwho.parent.parent
    pars     = parent.findAll('p')
    tags     = pars[1].contents
    for p in pars[2:]:
        tags.extend( p.contents )
    a = []
    s = {}
    timere = re.compile(' *\( *([^ ].*[^ ]) *...\) *')
    for entry in tags:
        #print '---', entry, '----', type(entry)
        if type(entry) == type([]):
            continue
        elif entry.find('b') != -1:
            # most unfortunate, pandas/ipython blows up printing the unicode
            if hasattr(entry,'text'):
                name      = unicodedata.normalize('NFKD', entry.text).encode('ascii','ignore')
                if len(name) > 0:
                    if name[0] == '*':
                        s['star'] = True
                        name = name[1:]
                    else:
                        s['star' ] = False
                    if 'name' in s:
                        s['name'] += name
                    else:
                        s['name'] = name
        elif timere.match( entry ):
            timestr   = string.strip( timere.match( entry ).group(1) )
            t         = parsetime( timestr )
            s['time'] = t
            s['name'] = namequirks( s['name'] )
            s['month'] = datetime.date(d).replace(day=1)
            a.append(s)
            s = {}
    a2 = { 'name' : Series([s['name'] for s in a]),
          'time' : Series(map(lambda s: s['time'], a)),
          'star' : Series(map(lambda s: s['star'], a )),
          'month' : Series(map(lambda s: s['month'], a )),
          'rank' : Series( range(1, len(a) + 1 ) ) }
    df = DataFrame(a2)
    
    return( df )

#today=datetime.today()
#df = DataFrame()
#for y in range(2005, 2012):
    #for m in range(1,13):
        #d = datetime(y,m,1)
        #if d <= today:
            #print y,m
            #df=df.append( pondersolvers(datetime(y,m,1)), ignore_index=True)
